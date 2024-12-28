import boto3
import botocore.exceptions
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Tuple

class S3Client:
    def __init__(self, access_key, secret_key, endpoint_url, parallelism=8):
        self.client = boto3.client('s3',
                                   aws_access_key_id=access_key,
                                   aws_secret_access_key=secret_key,
                                   endpoint_url=endpoint_url)
        self.parallelism = parallelism

    def upload(self, bucket, key, data):
        self.client.put_object(Bucket=bucket, Key=key, Body=data)
    
    def download(self, bucket_name, key) -> Tuple[bool, str]:
        content = None
        try:
            response = self.client.get_object(Bucket=bucket_name, Key=key)
            content = response['Body'].read().decode('utf-8')
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "NoSuchKey":
                return False, content
            raise e
        return True, content

    def batch_upload(self, bucket, files):
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
                    print(f"Error processing row {filename}: {e}")
