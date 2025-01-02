import argparse
from pyspark.sql import SparkSession
from utils.s3utils import S3Client
from utils.merizoutils import upsert_stats, format_parsed

def app(args):
    # Create a Spark session
    spark = SparkSession.builder.appName("MerizoSearchPipeline").getOrCreate()
    sc = spark.sparkContext

    # Define broadcast variables
    bc_args = sc.broadcast(args)

    # Define UDFs
    def merizo(partition):
        from utils.merizoutils import batch_search_and_parse
        from utils.s3utils import S3Client
        # run in batch to avoid being killed by merizo
        batch_size = 8
        results = []
        partition = list(partition)
        args = bc_args.value
        s3 = S3Client(endpoint_url=args.s3url, access_key=args.access_key,
                      secret_key=args.secret_key, parallelism=8)
        for i in range(0, len(partition), batch_size):
            batch = partition[i:i + batch_size]
            results += batch_search_and_parse(batch, s3, args.output_bucket,
                                              parallelism=2, retry=3)
        return results # [(id, mean, cath_ids)]
    
    def summary(d1, d2):
        from collections import defaultdict
        result = defaultdict(int)
        for k, v in d1.items():
            result[k] += v
        for k, v in d2.items():
            result[k] += v
        return dict(result)

    # Read all files into an RDD
    rdd = sc.wholeTextFiles(f"s3a://{args.input_bucket}/",
                            minPartitions=args.partitions)
    print("Number of partitions: ", rdd.getNumPartitions())
    if args.test:
        rdd = sc.parallelize(rdd.take(500), 30)
    result_rdd = rdd.mapPartitions(merizo)

    filtered_rdd = result_rdd.filter(lambda r: len(r[2]) > 0)
    filtered_rdd.cache()
    summary_dict = filtered_rdd.map(lambda r: r[2]).reduce(summary)

    filtered_rdd = filtered_rdd.map(lambda r: r[1])
    filtered_rdd.cache()
    mean = filtered_rdd.mean()
    std = filtered_rdd.stdev()

    summary_body = format_parsed(summary_dict, bodyonly=True, cath_key="cath_code")
    s3 = S3Client(endpoint_url=args.s3url,
                access_key=args.access_key,
                secret_key=args.secret_key)
    s3.upload(bucket=args.summary_bucket, key=args.summary_key, data=summary_body)
    upsert_stats(s3client=s3, bucket=args.summary_bucket, key="plDDT_means.csv",
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
    parser.add_argument("--partitions", type=int, default=300, help="Number of partitions to use")
    parser.add_argument("--test", action="store_true", help="Run the pipeline with a small subset of the data")
    args = parser.parse_args()
    
    app(args)