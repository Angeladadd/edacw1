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
        id = os.path.basename(filepath.rstrip(".pdb.gz"))
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

def main():
    dataset = sys.argv[1]
    spark = SparkSession.builder.appName("AnalysisPipelineApp").getOrCreate()

    sc = spark.sparkContext
    executor_info = sc._jsc.sc().statusTracker().getExecutorInfos()
    print("Worker Nodes:")
    for executor in executor_info:
        print(executor.host())

    # Read all files into an RDD
    rdd = sc.wholeTextFiles(f"hdfs://hostnode:9000/{dataset}/*.pdb.gz")
        # .withColumn("id", input_file_name()) \
        # .rdd
    result_rdd = sc.parallelize(rdd.take(20)) \
    .mapPartitions(search)

    result_rdd.cache()

    # result_rdd.map(format_output).saveAsHadoopFile(
    #     f"hdfs://hostnode:9000/{dataset}_parsed",
    #     outputFormatClass="org.apache.hadoop.mapreduce.lib.output.TextOutputFormat",
    #     keyClass="org.apache.hadoop.io.Text",
    #     valueClass="org.apache.hadoop.io.Text"
    # )

    # .map(parse_file)
    summary_dict = result_rdd.map(lambda r: r[2]) \
          .reduce(summary)

    # Show first 10 lines
    print("First 10 lines:")
    print(summary_dict)
    


if __name__ == "__main__":
    main()