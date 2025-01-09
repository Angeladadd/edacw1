import boto3
import botocore.exceptions
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Tuple
from tenacity import retry, stop_after_attempt, wait_fixed

class S3Client:
    """
    S3 client wrapper for uploading and downloading files(in batch)

    Args:
    - access_key: str, AWS access key
    - secret_key: str, AWS secret key
    - endpoint_url: str, S3 endpoint url
    - parallelism: int, number of parallel executors to upload files in batch
    """

    def __init__(self, access_key, secret_key, endpoint_url, parallelism=8):
        self.client = boto3.client('s3',
                                   aws_access_key_id=access_key,
                                   aws_secret_access_key=secret_key,
                                   endpoint_url=endpoint_url)
        self.parallelism = parallelism

    def upload(self, bucket, key, data):
        """
        Upload data to S3

        Args:
        - bucket: str, S3 bucket name
        - key: str, S3 key
        - data: str, data to upload
        """
        self.client.put_object(Bucket=bucket, Key=key, Body=data)
    
    @retry(stop=stop_after_attempt(3))
    def download(self, bucket_name, key) -> Tuple[bool, str]:
        """
        Download data from S3 with 3 times retry

        Args:
        - bucket_name: str, S3 bucket name
        - key: str, S3 key

        Returns:
        - fileexist: bool, True if file exists
        - content: str, file content

        Raises:
        - botocore.exceptions.ClientError: if any error occurs during file download
        """

        content = None
        try:
            response = self.client.get_object(Bucket=bucket_name, Key=key)
            content = response['Body'].read().decode('utf-8')
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "NoSuchKey":
                return False, content
            raise e
        return True, content

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
    def batch_upload(self, bucket, files):
        """
        Upload files in batch to S3 with 3 times retry

        Args:
        - bucket: str, S3 bucket name
        - files: list of (key, data) tuples

        Raises:
        - Exception: if any error occurs during file upload
        """

        with ThreadPoolExecutor(max_workers=self.parallelism) as executor:
            futures = {executor.submit(
                lambda key, data: self.upload(bucket, key, data),
                file[0], file[1]
                ): file for file in files}
            for future in as_completed(futures):
                filename, _ = futures[future]
                try:
                    future.result()
                except Exception as e:
                    logging.error(f"Error processing row {filename}: {e}")
                    raise e
