from io import StringIO
import os
import uuid
from concurrent.futures import ThreadPoolExecutor, wait
import shutil

def to_tsv_string(df):
    """
    Convert a pandas DataFrame to a TSV string

    Args:
    - df: pd.DataFrame

    Returns:
    - tsv_string: str
    """
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False, sep='\t')
    return csv_buffer.getvalue()


def batch_write_tmp_local(files, parallelism=4):
    """
    Batch write files to a temporary local directory

    Args:
    - files: list of (filename, content) tuples
    - parallelism: int, number of parallel executors

    Returns:
    - local_dir: str, path to the temporary local directory
    """
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
    """
    Clean up a temporary local directory

    Args:
    - local_dir: str, path to the temporary local directory
    """
    shutil.rmtree(local_dir)
