from subprocess import Popen, PIPE
import os
from collections import defaultdict
import json
from io import StringIO
import pandas as pd
import uuid
import shutil
from concurrent.futures import ThreadPoolExecutor, wait

#TODO: catch errors
def run_merizo_search(local_dir, output_prefix, parallelism=4):
    merizo = "/home/almalinux/merizo_search/merizo_search/merizo.py"
    db = "/home/almalinux/db/cath_foldclassdb/cath-4.3-foldclassdb"
    input = os.path.join(local_dir, "*.pdb")
    os.system(f"python3 {merizo} easy-search {input} {db} {output_prefix} tmp --iterate --output_headers -d cpu --threads {parallelism}")

def to_tsv_string(df):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False, sep='\t')
    return csv_buffer.getvalue()

def split_search_files(search_result):
    if not os.path.exists(search_result):
        return {}
    df = pd.read_csv(search_result, sep='\t')
    search_results = {} # id: (mean, cath_ids, (search_file_path, search_file_content))
    grouped = df.groupby(lambda x: df["query"][x].split("_merizo_")[0])
    for id, group in grouped:
        search_results[id] = (group["dom_plddt"].mean(), defaultdict(int),
                              (f"{id}_search.tsv", to_tsv_string(group)))
        for _, row in group.iterrows():
            data = json.loads(row["metadata"])
            search_results[id][1][data["cath"]] += 1
    return search_results

def split_segment_files(segment_result):
    if not os.path.exists(segment_result):
        return {}
    df = pd.read_csv(segment_result, sep='\t')
    segment_results = {} # id: (segment_file_path, segment_file_content)
    grouped = df.groupby("filename")
    for id, group in grouped:
        segment_results[id] = (f"{id}_segment.tsv", to_tsv_string(group))
    return segment_results

def parse_and_save(output_prefix, s3client, bucket): # [(id, mean, cath_ids)]
    search_results = split_search_files(f"{output_prefix}_search.tsv")
    segment_results = split_segment_files(f"{output_prefix}_segment.tsv")
    files = [file for _, (*_, file) in search_results.items()] + [file for _, file in segment_results.items()]
    files += [format_parsed(cath_ids=cath_ids, bodyonly=False,
                               id=id, mean=mean) for id, (mean, cath_ids, _) in search_results.items()]
    s3client.batch_upload(bucket, files)
    return [(id, mean, cath_id) for id, (mean, cath_id, _) in search_results.items()]

def upsert_stats(s3client, bucket, key, organism, mean, std):
    df = pd.DataFrame(columns=['organism', 'mean plddt', 'plddt std'])
    fileexist, csv_content = s3client.download(bucket, key)
    if fileexist:
        df = pd.read_csv(StringIO(csv_content))
    if organism in df['organism'].values:
        df.loc[df['organism'] == organism, 'mean plddt'] = mean
        df.loc[df['organism'] == organism, 'plddt std'] = std
    else:
        df = pd.concat([df, pd.DataFrame({'organism': [organism], 'mean plddt': [mean], 'plddt std': [std]})], ignore_index=True)
        
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3client.upload(bucket=bucket, key=key, data=csv_buffer.getvalue())


def batch_write_tmp_local(files, parallelism=4):
    local_dir = f"/tmp/merizo_{uuid.uuid4()}"
    os.makedirs(local_dir)
    def write_file(filename, content):
        file_path = os.path.join(local_dir, os.path.basename(filename))
        with open(file_path, 'w') as file:
            file.write(content)
    with ThreadPoolExecutor(max_workers=parallelism) as executor:
        futures = [executor.submit(write_file, filename, content) for filename, content in files]
        wait(futures)
    return local_dir

def clean_tmp_local(local_dir):
    shutil.rmtree(local_dir)

def format_parsed(cath_ids, bodyonly=False, id=None, mean=None, cath_key="cath_id"):
    body = f"{cath_key},count\n" + "\n".join([f"{cath},{v}" for cath, v in cath_ids.items()])
    if bodyonly:
        return body
    header = f"#{id} Results." + (f" mean plddt: {mean}" if mean else "mean plddt: 0")
    return (f"{id}.parsed", header + "\n" + body)