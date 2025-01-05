from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract
import argparse

def app(args):
    spark = SparkSession.builder.appName("GeneratePipelineRunReport").getOrCreate()
    sc = spark.sparkContext

    input_df = sc.wholeTextFiles(f"s3a://{args.input_bucket}/", 200).toDF(["filename", "content"])
    output_df = sc.wholeTextFiles(f"s3a://{args.output_bucket}/", 200).toDF(["filename", "content"])

    input_df = input_df.withColumn("id", regexp_extract(col("filename"), f"^s3a://{args.input_bucket}/(.*)\.pdb$", 1))
    
    parsed_df = output_df.filter(col("filename").endswith(".parsed")).withColumn("id", regexp_extract(col("filename"), f"^s3a://{args.output_bucket}/(.*)\.parsed$", 1))
    search_df = output_df.filter(col("filename").endswith("_search.tsv")).withColumn("id", regexp_extract(col("filename"), f"^s3a://{args.output_bucket}/(.*)_search\.tsv$", 1))
    segment_df = output_df.filter(col("filename").endswith("_segment.tsv")).withColumn("id", regexp_extract(col("filename"), f"^s3a://{args.output_bucket}/(.*)_segment\.tsv$", 1))

    # find input without segment files
    missing_segment = input_df.join(segment_df, "id", "left_anti").select("id")
    # find input without search files
    missing_search = input_df.join(search_df, "id", "left_anti").select("id")
    # find input without parsed files
    missing_parsed = input_df.join(parsed_df, "id", "left_anti").select("id")

    print(f"Num of segment.tsv: {segment_df.count()}")
    print(f"Num of search.tsv: {search_df.count()}")
    print(f"Num of parsed: {parsed_df.count()}")

    missing_segment.coalesce(1).write.mode("overwrite").csv(f"s3a://{args.validation_bucket}/{args.dataset}_missing_segment", header=True)
    missing_search.coalesce(1).write.mode("overwrite").csv(f"s3a://{args.validation_bucket}/{args.dataset}_missing_search", header=True)
    missing_parsed.coalesce(1).write.mode("overwrite").csv(f"s3a://{args.validation_bucket}/{args.dataset}_missing_parsed", header=True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("dataset", type=str, help="Dataset to analyse the pipeline results")
    parser.add_argument("input_bucket", type=str, help="The name of bucket that contains pdb files")
    parser.add_argument("output_bucket", type=str, help="The name of bucket that contains parsed and intermediate files")
    parser.add_argument("validation_bucket", type=str, help="The name of bucket to store the validation report")
    args = parser.parse_args()
    app(args)