from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract
import argparse
import os
import shutil

def app(args):
    spark = SparkSession.builder.appName("ResultValidation").getOrCreate()
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

    # Remove the temporary directory
    validation_dir = f"tmp/{args.dataset}_validation"
    if os.path.exists(validation_dir):
        shutil.rmtree(validation_dir)

    is_valid = missing_segment.count() == 0 and missing_search.count() == 0 and missing_parsed.count() == 0
    if is_valid:
        print("Validation passed without any missing files")
    else:
        assert missing_segment.count() == 0, "Missing segment files"
        assert missing_parsed.count() == missing_search.count(), "Number of missing parsed and search files are not equal"
        # Run merizo search and parse the results locally for random pdb files
        samples = input_df.sample(False, 0.1).limit(8).collect()
        # Download the pdb files from S3 in local directory
        os.makedirs(validation_dir, exist_ok=True)
        for sample in samples:
            with open(f"{validation_dir}/{sample.id}.pdb", "w") as f:
                f.write(sample.content)
        # Run merizo search and parse the results locally
        output_prefix = f"{validation_dir}/output"
        os.system(f"{args.python_path} {args.merizo_path} easy-search {validation_dir}/*.pdb {args.db_path} {output_prefix} tmp --iterate --output_headers -d cpu --threads 4")
        # Check if the search and segment file are empty
        search_output = f"{output_prefix}_search.tsv"
        if os.path.exists(search_output):
            with open(search_output, "r") as f:
                next(f) # skip the header
                # count the number of lines in the file
                entries = len(f.readlines())
                assert entries == 0, "Search file is not empty for missing _search.tsv files"


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("dataset", type=str, help="Dataset to analyse the pipeline results")
    parser.add_argument("input_bucket", type=str, help="The name of bucket that contains pdb files")
    parser.add_argument("output_bucket", type=str, help="The name of bucket that contains parsed and intermediate files")
    parser.add_argument("validation_bucket", type=str, help="The name of bucket to store the validation report")
    parser.add_argument("--python_path", type=str, default="/home/almalinux/pipeline/venv/bin/python", help="The path to python executable")
    parser.add_argument("--db_path", type=str, default="/home/almalinux/db/cath_foldclassdb/cath-4.3-foldclassdb", help="The path to the merizo.py script")
    parser.add_argument("--merizo_path", type=str, default="/home/almalinux/merizo_search/merizo_search/merizo.py", help="The path to the CATH database")
    args = parser.parse_args()
    app(args)