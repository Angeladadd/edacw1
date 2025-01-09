import argparse
from pyspark.sql import SparkSession
from utils.s3utils import S3Client
from utils.merizoutils import upsert_stats, format_parsed
from utils.metricsutils import write_metrics, MERIZO_FAILED_METRIC

def app(args):
    # Create a Spark session
    test_suffix = "Test" if args.test else ""
    spark = SparkSession.builder.appName("MerizoSearchPipeline" + test_suffix).getOrCreate()
    sc = spark.sparkContext

    # Define broadcast variables
    bc_args = sc.broadcast(args)

    # Initialise failure count
    write_metrics(MERIZO_FAILED_METRIC, 0)

    # Define UDFs
    def merizo(partition):
        """
        Run merizo search and parse the results for a partition

        Args:
        - partition: iterator of (file, content) tuples

        Returns:
        - results: list of (id, mean, cath_ids)
        """

        from utils.merizoutils import batch_search_and_parse
        from utils.s3utils import S3Client
        # run in batch to avoid being killed by merizo
        results = []
        partition = list(partition)
        args = bc_args.value # broadcast variables
        batch_size = args.merizo_batch_size
        s3_parallelism = 8
        retry = 3
        s3 = S3Client(endpoint_url=args.s3url, access_key=args.access_key,
                      secret_key=args.secret_key, parallelism=s3_parallelism)
        for i in range(0, len(partition), batch_size):
            batch = partition[i:i + batch_size]
            results += batch_search_and_parse(batch, s3, args.output_bucket,
                                              python_path=args.python_path,
                                              merizo_path=args.merizo_path,
                                              db_path=args.db_path,
                                              parallelism=args.merizo_thread_num,
                                              retry=retry)
        return results # [(id, mean, cath_ids)]
    
    def summary(d1, d2):
        """
        Count the number of occurrences of each key in two dictionaries

        Args:
        - d1: dict
        - d2: dict

        Returns:
        - result: dict
        """

        from collections import defaultdict
        result = defaultdict(int)
        for k, v in d1.items():
            result[k] += v
        for k, v in d2.items():
            result[k] += v
        return dict(result)

    # Read all files into RDDs with minPartitions
    rdd = sc.wholeTextFiles(f"s3a://{args.input_bucket}/",
                            minPartitions=args.partitions)
    print("Number of partitions: ", rdd.getNumPartitions())

    if args.test:
        # Run the pipeline with a small subset of the data for end2end test
        rdd = sc.parallelize(rdd.take(100), 12)

    # Run the pipeline: merizo search -> parse -> upload
    result_rdd = rdd.mapPartitions(merizo)

    # Filter out empty results and calculate summary statistics
    filtered_rdd = result_rdd.filter(lambda r: len(r[2]) > 0)
    filtered_rdd.cache()

    # Run the pipeline: summary
    summary_dict = filtered_rdd.map(lambda r: r[2]).reduce(summary)
    filtered_rdd = filtered_rdd.map(lambda r: r[1])
    filtered_rdd.cache()
    mean = filtered_rdd.mean()
    std = filtered_rdd.stdev()

    # Write summary to S3
    summary_body = format_parsed(summary_dict, bodyonly=True, cath_key="cath_code")
    s3 = S3Client(endpoint_url=args.s3url,
                access_key=args.access_key,
                secret_key=args.secret_key)
    s3.upload(bucket=args.summary_bucket, key=args.summary_key, data=summary_body)


    # Upsert mean and std to S3
    upsert_stats(s3client=s3, bucket=args.summary_bucket, key=args.mean_key,
                 organism=args.dataset, mean=mean, std=std)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("dataset", type=str, help="The dataset to process")
    parser.add_argument("s3url", type=str,
                        default="https://ucabc46-s3.comp0235.condenser.arc.ucl.ac.uk",
                        help="S3 endpoint URL that stores the data")
    parser.add_argument("access_key", type=str, help="The S3 access key")
    parser.add_argument("secret_key", type=str, help="The S3 secret key")
    parser.add_argument("input_bucket", type=str, help="The input bucket name")
    parser.add_argument("output_bucket", type=str, help="The output bucket name")
    parser.add_argument("summary_bucket", type=str, help="The summary bucket name")
    parser.add_argument("summary_key", type=str, help="The summary key")
    parser.add_argument("mean_key", type=str, help="The mean key")
    parser.add_argument("--python_path", type=str, default="/home/almalinux/pipeline/venv/bin/python", help="The path to the pipeline")
    parser.add_argument("--merizo_path", type=str, default="/home/almalinux/merizo_search/merizo_search/merizo.py", help="The path to the merizo.py script")
    parser.add_argument("--db_path", type=str, default="/home/almalinux/db/cath_foldclassdb/cath-4.3-foldclassdb", help="The path to the CATH database")
    parser.add_argument("--merizo_batch_size", type=int, default=8, help="Batch size for merizo search")
    parser.add_argument("--merizo_thread_num", type=int, default=2, help="Number of threads for merizo search")
    parser.add_argument("--partitions", type=int, default=300, help="Number of partitions to use")
    parser.add_argument("--test", action="store_true", help="Run the pipeline with a small subset of the data")
    args = parser.parse_args()
    
    app(args)