from pyspark.sql import SparkSession
import sys

def search(iter):
    from subprocess import Popen, PIPE
    import os
    from collections import defaultdict
    import json
    import statistics
    import csv

    local_input_dir = "/home/almalinux/input"

    if not os.path.exists(local_input_dir):
        os.makedirs(local_input_dir)

    def save_to_local(row):
        filepath, content = row[0], row[1]
        id = os.path.basename(filepath.rstrip(".pdb"))
        local_filename = os.path.join(local_input_dir, os.path.basename(f"{id}.pdb"))
        with open(local_filename, "w") as f:
            f.write(content)
        return id

    def search_file(id):
        cmd = ['python3',
            '/home/almalinux/merizo_search/merizo_search/merizo.py',
            'easy-search',
            os.path.join(local_input_dir, f"{id}.pdb"),
            '/home/almalinux/db/cath_foldclassdb/cath-4.3-foldclassdb',
            os.path.join(local_input_dir, f"{id}.pdb"),
            'tmp',
            '--iterate',
            '--output_headers',
            '-d',
            'cpu',
            '--threads',
            '1'
            ]
        p = Popen(cmd, stdin=PIPE,stdout=PIPE, stderr=PIPE)
        p.wait()
        _, err = p.communicate()
        if err:
            print(err)
            # raise Exception(str(err) + str(local_ip))
    def parse_file(id):
        local_input_dir = "/home/almalinux/input"
        cath_ids = defaultdict(int)
        plDDT_values = []
        search_file = os.path.join(local_input_dir, f"{id}.pdb_search.tsv")
        if not os.path.exists(search_file):
            return (id, 0, cath_ids)
        with open(search_file, "r") as fhIn:
            next(fhIn)
            msreader = csv.reader(fhIn, delimiter='\t',) 
            for _, row in enumerate(msreader):
                plDDT_values.append(float(row[3]))
                meta = row[15]
                data = json.loads(meta)
                cath_ids[data["cath"]] += 1
        mean = statistics.mean(plDDT_values) if plDDT_values else 0
        return (id, mean, cath_ids)

    for row in iter:
        id = save_to_local(row)
        search_file(id)
        yield parse_file(id)

def format_output(row):
    id, mean, cath_ids = row
    header = f"#{id} Results." + (f"mean plddt: {mean}" if mean else "mean plddt: 0")
    body = "cath_id,count\n" + "\n".join([f"{cath},{v}" for cath, v in cath_ids.items()])
    return (f"{id}.pdb.parsed", header + "\n" + body)

def summary(d1, d2):
    from collections import defaultdict
    result = defaultdict(int)
    for k, v in d1.items():
        result[k] += v
    for k, v in d2.items():
        result[k] += v
    return dict(result)

def save_summary(spark, path, summary_dict):
    data = [(k, v) for k, v in summary_dict.items()]
    df = spark.createDataFrame(data, ["cath_id", "count"])
    df.coalesce(1).write \
        .option("header", "true") \
        .mode("overwrite") \
        .csv(path)


def main():
    passwd = None
    with open('/home/almalinux/miniopass', 'r') as file:
        passwd = file.read().strip()

    dataset = sys.argv[1]
    spark = SparkSession.builder \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .appName("AnalysisPipelineApp") \
    .getOrCreate()
    
    sc = spark.sparkContext
    broadcast_passwd = sc.broadcast(passwd)

    def save_rdd_by_key(partition):
        import boto3

        passwd = broadcast_passwd.value
        s3 = boto3.client("s3",
                endpoint_url="https://ucabc46-s3.comp0235.condenser.arc.ucl.ac.uk",
                aws_access_key_id="myminioadmin",
                aws_secret_access_key=passwd)
        bucket = "human-cath-parsed"
        for row in partition:
            key, data = row
            s3.put_object(Bucket=bucket, Key=key, Body=data)

    # sc.setLogLevel("DEBUG")
    executor_info = sc._jsc.sc().statusTracker().getExecutorInfos()
    print("Worker Nodes:")
    for executor in executor_info:
        print(executor.host())

    # https://medium.com/@abdullahdurrani/working-with-minio-and-spark-8b4729daec6e
    # Set the MinIO access key, secret key, endpoint, and other configurations
    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", "myminioadmin")
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", passwd)
    sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "https://ucabc46-s3.comp0235.condenser.arc.ucl.ac.uk")
    sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
    sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")


    # Read all files into an RDD
    rdd = sc.wholeTextFiles("s3a://human-alphafolddb/UP000005640_9606_HUMAN_v4/")
    
    result_rdd =  sc.parallelize(rdd.take(100)) \
    .mapPartitions(search)

    result_rdd.cache()

    # # TODO: save to minio

    result_rdd.map(format_output) \
        .foreachPartition(save_rdd_by_key)
    # .map(parse_file)
    summary_dict = result_rdd.map(lambda r: r[2]) \
          .reduce(summary)

    mean = result_rdd.filter(lambda r: len(r[2]) > 0) \
    .map(lambda r: r[1]) \
    .map(lambda x: (x, 1)) \
    .reduce(lambda a, b: (a[0] + b[0], a[1] + b[1]))

    print("First 100 files:")
    print("summary: ", summary_dict)
    print("mean: ", mean[0] / mean[1])

    save_summary(spark, f"s3a://summary/{dataset}_summary.csv", summary_dict)


    


if __name__ == "__main__":
    main()