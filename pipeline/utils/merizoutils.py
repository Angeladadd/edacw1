from subprocess import Popen, PIPE
import os
from collections import defaultdict
import json
import statistics
import csv
from io import StringIO
import pandas as pd

def run_merizo_search(local_filepath):
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

def parse_result(local_input_dir, id):
    cath_ids = defaultdict(int)
    plDDT_values = []
    search_file = os.path.join(local_input_dir, f"{id}.pdb_search.tsv")
    if not os.path.exists(search_file):
        return 0, cath_ids
    with open(search_file, "r") as fhIn:
        next(fhIn)
        msreader = csv.reader(fhIn, delimiter='\t',) 
        for _, row in enumerate(msreader):
            plDDT_values.append(float(row[3]))
            meta = row[15]
            data = json.loads(meta)
            cath_ids[data["cath"]] += 1
    mean = statistics.mean(plDDT_values) if plDDT_values else 0
    return mean, cath_ids

def upsert_stats(s3client, bucket, key, organism, mean, std):
    """
    Update the mean and std of the organism
    Specified for merizo search
    """
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


def write_local(local_filepath, content):
    with open(local_filepath, "w") as f:
        f.write(content)

def format_output(cath_ids, bodyonly=False, id=None, mean=None, cath_key="cath_id"):
    body = f"{cath_key},count\n" + "\n".join([f"{cath},{v}" for cath, v in cath_ids.items()])
    if bodyonly:
        return body
    header = f"#{id} Results." + (f"mean plddt: {mean}" if mean else "mean plddt: 0")
    return (f"{id}.parsed", header + "\n" + body)