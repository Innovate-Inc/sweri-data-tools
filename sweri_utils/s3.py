import logging
import boto3
from botocore.exceptions import ClientError
import os

def get_s3_resource():
    sso_profile_name = os.getenv('AWS_SSO_PROFILE_NAME', None)
    if sso_profile_name:
        session = boto3.Session(profile_name=sso_profile_name)
        s3 = session.resource('s3')
    else:
        s3 = boto3.resource('s3')
    return s3


def upload_to_s3(bucket, file_name, obj_name):
    s3 = boto3.client('s3')
    try:
        s3.upload_file(file_name, bucket, obj_name)
    except ClientError as e:
        logging.error(e)


def import_s3_csv_to_postgres_table(conn, db_schema, fields, destination_table, s3_bucket, csv_file,
                                    aws_region='us-west-2'):
    cursor = conn.cursor()
    with conn.transaction():
        # delete existing data in destination table
        cursor.execute(f'DELETE FROM {db_schema}.{destination_table};')
        # use aws_s3 extension to insert data from the csv file in s3 into the postgres table
        cursor.execute(f"""SELECT aws_s3.table_import_from_s3('{db_schema}.{destination_table}', '{",".join(fields)}', '(format csv, HEADER)', aws_commons.create_s3_uri('{s3_bucket}', '{csv_file}', '{aws_region}'));""")
        logging.info(f'completed import of {csv_file} into {db_schema}.{destination_table}')


def delete_bucket_contents(bucket_name, prefix):
    if not prefix:
        error_msg = f'Deleting {bucket_name} without a prefix is not allowed.'
        logging.error(error_msg)
        raise ValueError(error_msg)

    s3 = get_s3_resource()
    bucket = s3.Bucket(bucket_name)
    bucket.objects.filter(Prefix=prefix).delete()
