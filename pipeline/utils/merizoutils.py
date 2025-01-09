import os
from collections import defaultdict
import json
from io import StringIO
import pandas as pd
import logging
from .ioutils import to_tsv_string, batch_write_tmp_local, clean_tmp_local
from .metricsutils import write_metrics, MERIZO_FAILED_METRIC


def batch_search_and_parse(
        files, s3client, output_bucket,
        python_path, merizo_path, db_path,
        parallelism=4, retry=3):
    """
    1. Run merizo search in batch
        1.1 If failed, run search one by one with retry
    2. Parse _search.tsv and _segment.tsv
    3. Save parsed results to s3
    4. Return results [(id, mean, cath_ids)]

    Args:
    - files: list of (file, content) tuples
    - s3client: S3Client
    - output_bucket: str, s3 bucket to save parsed results
    - python_path: str, path to python
    - merizo_path: str, path to merizo.py
    - db_path: str, path to database
    - parallelism: int, number of threads for merizo search
    - retry: int, number of retries for failed search

    Returns:
    - results: list of (id, mean, cath_ids)
    """

    local_dir = batch_write_tmp_local(files, parallelism)
    output_prefix = f"{local_dir}/output"
    code = run_merizo_search(local_dir, output_prefix,
                             python_path, merizo_path, db_path, parallelism)
    output_prefixs = []
    if code != 0:
        failed_count = 0
        logging.error(f"merizo search failed with exit code {code}.")
        # run search one by one with retry
        for file, _ in files:
            local_path = os.path.join(local_dir, os.path.basename(file))
            id = os.path.basename(file).strip(".pdb")
            single_prefix = f"{local_dir}/{id}"
            for i in range(retry):
                logging.warning(f"{i}-th retry: merizo search for {file}...")
                code = run_merizo_search(local_path, single_prefix,
                                         python_path, merizo_path, db_path, parallelism)
                if code == 0:
                    logging.warning(f"merizo search retry for {file} succeeded.")
                    output_prefixs.append(single_prefix)
                    break 
                if i == retry - 1:
                    failed_count += 1
                    logging.error(f"merizo search for {file} failed with {retry} times retry.")
        write_metrics(MERIZO_FAILED_METRIC, failed_count)
    else:
        output_prefixs.append(output_prefix)
    results = parse_and_save(output_prefixs, s3client, output_bucket)
    clean_tmp_local(local_dir)
    return results

def run_merizo_search(local_path, output_prefix, python, merizo, db, parallelism=4):
    """
    Shell command to run merizo search

    Args:
    - local_path: str, path to local directory containing pdb files
    - output_prefix: str, prefix for output files
    - python: str, path to python
    - merizo: str, path to merizo.py
    - db: str, path to database
    - parallelism: int, number of threads for merizo search

    Returns:
    - code: int, exit code of the shell command
    """
    input = local_path if local_path.endswith(".pdb") else os.path.join(local_path, "*.pdb")
    code = os.system(f"{python} -W ignore::FutureWarning {merizo} easy-search {input} {db} {output_prefix} tmp --iterate --output_headers -d cpu --threads {parallelism}")
    return code

def split_search_files(search_result):
    """
    Split search results in a batch _search.tsv file into individual _search.tsv files
    and parse the content into a dictionary.

    Args:
    - search_result: str, path to _search.tsv file

    Returns:
    - search_results: dict, id: (mean, cath_ids, (search_file_path, search_file_content))
    """

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
    """
    Split segment results in a batch _segment.tsv file into individual _segment.tsv files

    Args:
    - segment_result: str, path to _segment.tsv file

    Returns:
    - segment_results: dict, id: (segment_file_path, segment_file_content)
    """

    if not os.path.exists(segment_result):
        return {}
    df = pd.read_csv(segment_result, sep='\t')
    segment_results = {} # id: (segment_file_path, segment_file_content)
    grouped = df.groupby("filename")
    for id, group in grouped:
        segment_results[id] = (f"{id}_segment.tsv", to_tsv_string(group))
    return segment_results

def parse_and_save(output_prefixs, s3client, bucket): # [(id, mean, cath_ids)]
    """
    For all output_prefixs, parse _search.tsv and _segment.tsv files and save to s3

    Args:
    - output_prefixs: list of str, output_prefixs for merizo search results
    - s3client: S3Client
    - bucket: str, s3 bucket to save parsed results

    Returns:
    - results: list of (id, mean, cath_ids)
    """
    files = []
    results = []
    for output_prefix in output_prefixs:
        search_results = split_search_files(f"{output_prefix}_search.tsv")
        segment_results = split_segment_files(f"{output_prefix}_segment.tsv")
        files += [file for _, (*_, file) in search_results.items()] + [file for _, file in segment_results.items()]
        files += [format_parsed(cath_ids=cath_ids, bodyonly=False,
                                id=id, mean=mean) for id, (mean, cath_ids, _) in search_results.items()]
        results += [(id, mean, cath_ids) for id, (mean, cath_ids, _) in search_results.items()]
    s3client.batch_upload(bucket, files)
    return results

def upsert_stats(s3client, bucket, key, organism, mean, std):
    """
    Insert/Update mean and std of plddt for a database in S3

    Args:
    - s3client: S3Client
    - bucket: str, s3 bucket to save summary stats
    - key: str, s3 key to save summary stats
    - organism: str, organism name/dataset name
    - mean: float, mean plddt
    - std: float, std of plddt
    """

    df = pd.DataFrame(columns=["organism", "mean plddt", "plddt std"])
    fileexist, csv_content = s3client.download(bucket, key)
    if fileexist:
        df = pd.read_csv(StringIO(csv_content))
    if organism in df["organism"].values:
        df.loc[df["organism"] == organism, "mean plddt"] = mean
        df.loc[df["organism"] == organism, "plddt std"] = std
    else:
        df = pd.concat([df, pd.DataFrame(
            {"organism": [organism], "mean plddt": [mean], "plddt std": [std]
             })], ignore_index=True)
        
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3client.upload(bucket=bucket, key=key, data=csv_buffer.getvalue())

def format_parsed(cath_ids, bodyonly=False, id=None, mean=None, cath_key="cath_id"):
    """
    Format cath_ids dict into a csv string(.parsed file)

    Args:
    - cath_ids: dict, cath_id: count
    - bodyonly: bool, return body only if True
    - id: str, id of the parsed file
    - mean: float, mean plddt
    - cath_key: str, key for cath_id

    Returns:
    - parsed: str, formatted parsed content
    """

    body = f"{cath_key},count\n" + "\n".join([f"{cath},{v}" for cath, v in cath_ids.items()])
    if bodyonly:
        return body
    header = f"#{id} Results." + (f" mean plddt: {mean}" if mean else "mean plddt: 0")
    return (f"{id}.parsed", header + "\n" + body)