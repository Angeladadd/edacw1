import argparse
from pyspark.sql import SparkSession
from utils.s3utils import S3Client
from utils.merizoutils import upsert_stats, format_parsed

def app(config):
    # Create a Spark session
    spark = SparkSession.builder \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .appName("MerizoSearchPipeline") \
    .getOrCreate()
    
    # Setup S3A access
    sc = spark.sparkContext
    # https://medium.com/@abdullahdurrani/working-with-minio-and-spark-8b4729daec6e
    # Set the MinIO access key, secret key, endpoint, and other configurations
    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", config["s3"]["access_key"])
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", config["s3"]["secret_key"])
    sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", config["s3"]["endpoint_url"])
    sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
    sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    # Define broadcast variables
    bc_config = sc.broadcast(config)

    # Define UDFs
    def merizo(partition):
        from utils.merizoutils import batch_search_and_parse
        from utils.s3utils import S3Client
        # run in batch to avoid being killed by merizo
        batch_size = 8
        results = []
        partition = list(partition)
        config = bc_config.value
        s3 = S3Client(endpoint_url=config["s3"]["endpoint_url"], access_key=config["s3"]["access_key"],
                      secret_key=config["s3"]["secret_key"], parallelism=8)
        for i in range(0, len(partition), batch_size):
            batch = partition[i:i + batch_size]
            results += batch_search_and_parse(batch, s3, config["dataset"]["output_bucket"],
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
    rdd = sc.wholeTextFiles(f"s3a://{config['dataset']['input_bucket']}/",
                            minPartitions=config["dataset"]["partitions"])
    print("Number of partitions: ", rdd.getNumPartitions())
    if config["test"]:
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
    s3 = S3Client(endpoint_url=config["s3"]["endpoint_url"],
                access_key=config["s3"]["access_key"],
                secret_key=config["s3"]["secret_key"])
    bucket = config["dataset"]["summary_bucket"]
    s3.upload(bucket=bucket, key=config["dataset"]["summary_key"], data=summary_body)
    upsert_stats(s3client=s3, bucket=bucket, key="plDDT_means.csv",
                 organism=config["dataset"]["name"], mean=mean, std=std)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("dataset", type=str, help="The dataset to process")
    parser.add_argument("--partitions", type=int, default=1200, help="Number of partitions to use")
    parser.add_argument("--test", action="store_true", help="Run the pipeline with a small subset of the data")
    args = parser.parse_args()
    
    # Process arguments
    passwd = None
    with open('/home/almalinux/miniopass', 'r') as file:
        passwd = file.read().strip()

    config = {
        "test": args.test,
        "s3": {
            "access_key": "myminioadmin",
            "secret_key": passwd,
            "endpoint_url": "https://ucabc46-s3.comp0235.condenser.arc.ucl.ac.uk",
        },
        "dataset": {
            "name": args.dataset,
            "input_bucket": f"{args.dataset}-alphafolddb",
            "output_bucket": f"{args.dataset}-cath-parsed-test",
            "summary_bucket": "cath-summary",
            "summary_key": f"{args.dataset}_cath_summary.csv",
            "partitions": args.partitions,
        }
    }
    app(config)