from subprocess import Popen, PIPE
import os
from collections import defaultdict
import json
import statistics
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

def parse_results(output_prefix):
    result_map = {} # id: (mean, cath_ids)
    search_file = f"{output_prefix}_search.tsv"
    if not os.path.exists(search_file):
        return []
    df = pd.read_csv(search_file, sep='\t')[['query', 'dom_plddt', 'metadata']]
    for _, row in df.iterrows():
        id = row['query'].split('_merizo_')[0]
        data = json.loads(row['metadata'])
        if id not in result_map:
            result_map[id] = ([], defaultdict(int))
        result_map[id][0].append(float(row['dom_plddt']))
        result_map[id][1][data['cath']] += 1
    results = []
    for id, (plDDT_values, cath_ids) in result_map.items():
        results.append((id, statistics.mean(plDDT_values) if plDDT_values else 0, cath_ids))
    return results

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

def format_output(cath_ids, bodyonly=False, id=None, mean=None, cath_key="cath_id"):
    body = f"{cath_key},count\n" + "\n".join([f"{cath},{v}" for cath, v in cath_ids.items()])
    if bodyonly:
        return body
    header = f"#{id} Results." + (f" mean plddt: {mean}" if mean else "mean plddt: 0")
    return (f"{id}.parsed", header + "\n" + body)