import os

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv("docker/.env")  # rode sempre a partir da raiz do projeto

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id=os.environ["MINIO_USER"],
    aws_secret_access_key=os.environ["MINIO_PASSWORD"],
    region_name="us-east-1",
)

for bucket in ["landing", "warehouse"]:
    try:
        s3.head_bucket(Bucket=bucket)
        print(f"ja existe : {bucket}")
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchBucket"):
            s3.create_bucket(Bucket=bucket)
            print(f"criado    : {bucket}")
        else:
            raise
