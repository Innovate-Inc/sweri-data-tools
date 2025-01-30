import logging
import boto3
from botocore.exceptions import ClientError

def upload_to_s3(bucket, file_name, obj_name):
    s3 = boto3.client('s3')
    try:
        s3.upload_file(file_name, bucket, obj_name)
    except ClientError as e:
        logging.error(e)


