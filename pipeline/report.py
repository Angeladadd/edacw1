from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract
import argparse
from pyspark.sql.types import StringType

def app(config):
    spark = SparkSession.builder \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .appName("GeneratePipelineRunReport") \
    .getOrCreate()
    sc = spark.sparkContext
    # https://medium.com/@abdullahdurrani/working-with-minio-and-spark-8b4729daec6e
    # Set the MinIO access key, secret key, endpoint, and other configurations
    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", config["s3"]["access_key"])
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", config["s3"]["secret_key"])
    sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", config["s3"]["endpoint_url"])
    sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
    sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    input_df = sc.wholeTextFiles(f"s3a://human-alphafolddb/", 200).toDF(["filename", "content"])
    output_df = sc.wholeTextFiles(f"s3a://human-cath-parsed/", 200).toDF(["filename", "content"])

    input_df = input_df.withColumn("id", regexp_extract(col("filename"), "^s3a://human-alphafolddb/(.*)\.pdb$", 1))
    
    parsed_df = output_df.filter(col("filename").endswith(".parsed")).withColumn("id", regexp_extract(col("filename"), "^s3a://human-cath-parsed/(.*)\.parsed$", 1))
    search_df = output_df.filter(col("filename").endswith("_search.tsv")).withColumn("id", regexp_extract(col("filename"), "^s3a://human-cath-parsed/(.*)_search\.tsv$", 1))
    segment_df = output_df.filter(col("filename").endswith("_segment.tsv")).withColumn("id", regexp_extract(col("filename"), "^s3a://human-cath-parsed/(.*)_segment\.tsv$", 1))

    # find input without segment files
    missing_segment = input_df.join(segment_df, "id", "left_anti").select("id")
    # find input without search files
    missing_search = input_df.join(search_df, "id", "left_anti").select("id")
    # find input without parsed files
    missing_parsed = input_df.join(parsed_df, "id", "left_anti").select("id")

    missing_segment.coalesce(1).write.mode("overwrite").csv("s3a://report/missing_segment", header=True)
    missing_search.coalesce(1).write.mode("overwrite").csv("s3a://report/missing_search", header=True)
    missing_parsed.coalesce(1).write.mode("overwrite").csv("s3a://report/missing_parsed", header=True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    args = parser.parse_args()
    
    # Process arguments
    passwd = None
    with open('/home/almalinux/miniopass', 'r') as file:
        passwd = file.read().strip()

    config = {
        "s3": {
            "access_key": "myminioadmin",
            "secret_key": passwd,
            "endpoint_url": "https://ucabc46-s3.comp0235.condenser.arc.ucl.ac.uk",
        }
    }
    app(config)