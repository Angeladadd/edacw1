from pyspark.sql import SparkSession
import sys
import boto3
import botocore.exceptions
import pandas as pd
from io import StringIO

def format_output(row):
    id, mean, cath_ids = row
    header = f"#{id} Results." + (f"mean plddt: {mean}" if mean else "mean plddt: 0")
    body = "cath_id,count\n" + "\n".join([f"{cath},{v}" for cath, v in cath_ids.items()])
    return (f"{id}.parsed", header + "\n" + body)

def summary(d1, d2):
    from collections import defaultdict
    result = defaultdict(int)
    for k, v in d1.items():
        result[k] += v
    for k, v in d2.items():
        result[k] += v
    return dict(result)

def upsert_mean(s3, database, mean, std, bucket_name='cath-summary', file_key='plDDT_means.csv'):
    try:
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        csv_content = response['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(csv_content))
    except botocore.exceptions.ClientError as e:
        # If the file doesn't exist, create a new DataFrame with the required structure
        if e.response['Error']['Code'] == 'NoSuchKey':
            df = pd.DataFrame(columns=['organism', 'mean plddt', 'plddt std'])
        else:
            raise e  # If it's another error, re-raise it
    if database in df['organism'].values:
        df.loc[df['organism'] == database, 'mean plddt'] = mean
        df.loc[df['organism'] == database, 'plddt std'] = std
    else:
        df = pd.concat([df, pd.DataFrame({'organism': [database], 'mean plddt': [mean], 'plddt std': [std]})], ignore_index=True)
    
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=bucket_name, Key=file_key, Body=csv_buffer.getvalue())

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
        from subprocess import Popen, PIPE
        import os
        from collections import defaultdict
        import json
        import statistics
        import csv

        # Create a local folder for merizo search input/output
        local_input_dir = "/tmp/input"
        if not os.path.exists(local_input_dir):
            os.makedirs(local_input_dir)

        def local_file(row):
            filepath, content = row
            id = os.path.basename(filepath.rstrip(".pdb"))
            local_filepath = os.path.join(local_input_dir, os.path.basename(f"{id}.pdb"))
            with open(local_filepath, "w") as f:
                f.write(content)
            return (id, local_filepath)

        def search(local_filepath):
            cmd = ['python3',
                '/home/almalinux/merizo_search/merizo_search/merizo.py',
                'easy-search',
                local_filepath,
                '/home/almalinux/db/cath_foldclassdb/cath-4.3-foldclassdb',
                local_filepath,
                'tmp',
                '--iterate',
                '--output_headers',
                '-d',
                'cpu',
                '--threads',
                '1'
                ]
            p = Popen(cmd, stdin=PIPE,stdout=PIPE, stderr=PIPE)
            _, err = p.communicate()
            if err:
                print(err)
                # raise Exception(str(err) + str(local_ip))
        
        def parse(id):
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

        id, local_filepath = local_file(row)
        search(local_filepath)
        return parse(id)
    
    def save_to_s3(partition):
        import boto3
        from concurrent.futures import ThreadPoolExecutor, as_completed
        # Restore broadcast variables
        passwd = broadcast_passwd.value
        dataset = broadcast_dataset.value
        # Create S3 client
        s3 = boto3.client("s3",
            endpoint_url="https://ucabc46-s3.comp0235.condenser.arc.ucl.ac.uk",
            aws_access_key_id="myminioadmin",
            aws_secret_access_key=passwd)
        bucket = f"{dataset}-cath-parsed"

        def process(row):
            key, data = format_output(row)
            s3.put_object(Bucket=bucket, Key=key, Body=data)
            return result
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = {executor.submit(process, row): row for row in partition}
            for future in as_completed(futures):
                row = futures[future]  # Get the corresponding row for the future
                try:
                    result = future.result()  # Retrieve the result from the future
                except Exception as e:
                    print(f"Error processing row {row}: {e}")
                    # results.append((row, None))  # Append None for failed rows
        return

    # Read all files into an RDD
    rdd = sc.wholeTextFiles(f"s3a://{dataset}-alphafolddb/", minPartitions=120)
    
    print("Number of partitions: ", rdd.getNumPartitions())
    result_rdd = rdd.map(search_and_parse)
    # result_rdd = sc.parallelize(rdd.take(200)).map(search_and_parse)

    result_rdd.cache()
    result_rdd.foreachPartition(save_to_s3)
    # .map(parse_file)
    filtered_rdd = result_rdd.filter(lambda r: len(r[2]) > 0)
    summary_dict = filtered_rdd.map(lambda r: r[2]).reduce(summary)

    filtered_rdd = filtered_rdd.map(lambda r: r[1])
    filtered_rdd.cache()
    mean = filtered_rdd.mean()
    std = filtered_rdd.stdev()

    summary_body = "cath_id,count\n" + "\n".join([f"{cath},{v}" for cath, v in summary_dict.items()])
    s3 = boto3.client("s3",
                endpoint_url="https://ucabc46-s3.comp0235.condenser.arc.ucl.ac.uk",
                aws_access_key_id="myminioadmin",
                aws_secret_access_key=passwd)
    bucket = "cath-summary"
    s3.put_object(Bucket=bucket, Key=f"{dataset}_summary.csv", Body=summary_body)
    upsert_mean(s3, dataset, mean, std)


if __name__ == "__main__":
    app()