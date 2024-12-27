from pyspark.sql import SparkSession
import sys
from utils.s3utils import S3Client
from utils.merizoutils import upsert_stats, format_output

def app():
    # Process arguments
    dataset = sys.argv[1]
    passwd = None
    with open('/home/almalinux/miniopass', 'r') as file:
        passwd = file.read().strip()

    # Create a Spark session
    spark = SparkSession.builder \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .appName("AnalysisPipelineApp") \
    .getOrCreate()
    
    # Setup S3A access
    sc = spark.sparkContext
    # https://medium.com/@abdullahdurrani/working-with-minio-and-spark-8b4729daec6e
    # Set the MinIO access key, secret key, endpoint, and other configurations
    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", "myminioadmin")
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", passwd)
    sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "https://ucabc46-s3.comp0235.condenser.arc.ucl.ac.uk")
    sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
    sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    # Define broadcast variables
    broadcast_passwd = sc.broadcast(passwd)
    broadcast_dataset = sc.broadcast(dataset)

    # Define UDFs
    def search_and_parse(row):
        import os
        from utils.merizoutils import run_merizo_search, parse_result, write_local

        # Create a local folder for merizo search input/output
        local_input_dir = "/tmp/input"
        if not os.path.exists(local_input_dir):
            os.makedirs(local_input_dir)

        filepath, content = row
        id = os.path.basename(filepath.rstrip(".pdb"))
        local_filepath = os.path.join(local_input_dir, os.path.basename(f"{id}.pdb"))
        write_local(local_filepath, content)
        run_merizo_search(local_filepath)
        mean, cath_ids = parse_result(local_input_dir, id)
        return (id, mean, cath_ids)
    
    def save_to_s3(partition):
        from utils.s3utils import S3Client
        from utils.merizoutils import format_output
        # Restore broadcast variables
        passwd = broadcast_passwd.value
        dataset = broadcast_dataset.value
        # Create S3 client
        s3 = S3Client(endpoint_url="https://ucabc46-s3.comp0235.condenser.arc.ucl.ac.uk",
                    access_key="myminioadmin",
                    secret_key=passwd)
        bucket = f"{dataset}-cath-parsed"
        files = [format_output(cath_ids=row[2],
                               bodyonly=False,
                               id=row[0],
                               mean=row[1]
                               ) for row in partition]
        s3.batch_upload(bucket, files)
        return
    
    def summary(d1, d2):
        from collections import defaultdict
        result = defaultdict(int)
        for k, v in d1.items():
            result[k] += v
        for k, v in d2.items():
            result[k] += v
        return dict(result)

    # Read all files into an RDD
    rdd = sc.wholeTextFiles(f"s3a://{dataset}-alphafolddb/", minPartitions=120)
    
    print("Number of partitions: ", rdd.getNumPartitions())
    result_rdd = rdd.map(search_and_parse)
    # result_rdd = sc.parallelize(rdd.take(100)).map(search_and_parse)

    result_rdd.cache()
    result_rdd.foreachPartition(save_to_s3)

    filtered_rdd = result_rdd.filter(lambda r: len(r[2]) > 0)
    summary_dict = filtered_rdd.map(lambda r: r[2]).reduce(summary)

    filtered_rdd = filtered_rdd.map(lambda r: r[1])
    filtered_rdd.cache()
    mean = filtered_rdd.mean()
    std = filtered_rdd.stdev()

    summary_body = format_output(summary_dict, bodyonly=True, cath_key="cath_code")
    s3 = S3Client(endpoint_url="https://ucabc46-s3.comp0235.condenser.arc.ucl.ac.uk",
                access_key="myminioadmin",
                secret_key=passwd)
    bucket = "cath-summary"
    s3.upload(bucket=bucket, key=f"{dataset}_cath_summary.csv", data=summary_body)
    upsert_stats(s3client=s3, bucket=bucket, key="plDDT_means.csv", organism=dataset, mean=mean, std=std)


if __name__ == "__main__":
    app()