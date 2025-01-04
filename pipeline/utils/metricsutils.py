import os

TEXTFILE_DIR = "/var/lib/node_exporter/textfile_collector/"
METRIC_FILE = "pipeline_metrics.prom"

MERIZO_FAILED_METRIC = "merizo_search_failed"

def write_metrics(key, value):
    with open(os.path.join(TEXTFILE_DIR, METRIC_FILE), 'w') as f:
        f.write(f"{key} {value}\n")