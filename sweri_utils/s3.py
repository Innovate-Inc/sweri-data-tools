import logging
import boto3
from botocore.exceptions import ClientError

from sweri_utils.sweri_logging import log_this


@log_this
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