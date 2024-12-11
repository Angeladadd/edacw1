from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name
import sys
from spark_udfs import search_partition

def main():
    dataset = sys.argv[1]
    spark = SparkSession.builder.appName("Pipeline").getOrCreate()

    # Read all files into an RDD
    rdd = spark.sparkContext.wholeTextFiles(f"hdfs://hostnode:9000/{dataset}/*.pdb.gz")
        # .withColumn("id", input_file_name()) \
        # .rdd
    result_rdd = rdd.mapPartitions(search_partition)

    # Show first 10 lines
    for line in result_rdd.take(1):
        print(line)




if __name__ == "__main__":
    main()