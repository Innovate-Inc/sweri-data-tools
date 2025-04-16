import logging
import boto3
from botocore.exceptions import ClientError

def upload_to_s3(bucket, file_name, obj_name):
    s3 = boto3.client('s3')
    try:
        s3.upload_file(file_name, bucket, obj_name)
    except ClientError as e:
        logging.error(e)


def import_s3_csv_to_postgres_table(cursor, db_schema, fields, destination_table, s3_bucket, csv_file,
                                    aws_region='us-west-2'):
    cursor.execute('BEGIN;')
    # delete existing data in destination table
    cursor.execute(f'DELETE FROM {db_schema}.{destination_table};')
    # use aws_s3 extension to insert data from the csv file in s3 into the postgres table
    cursor.execute(f"""SELECT aws_s3.table_import_from_s3('{db_schema}.{destination_table}', '{",".join(fields)}', '(format csv, HEADER)', aws_commons.create_s3_uri('{s3_bucket}', '{csv_file}', '{aws_region}'));""")
    cursor.execute('COMMIT;')
    logging.info(f'completed import of {csv_file} into {db_schema}.{destination_table}')

def fetch_secrets(secret_name, out_file=".env",region_name="us-west-2"):
    """
    Fetches a secret from AWS Secrets Manager and writes it to a specified file.

    This method retrieves the secret value associated with the given `secret_name`
    from AWS Secrets Manager in the specified `region_name`. The secret is then
    written to the file specified by `out_file`.

    Args:
        secret_name (str): The name of the secret to retrieve.
        out_file (str, optional): The file path where the secret will be written. Defaults to ".env".
        region_name (str, optional): The AWS region where the secret is stored. Defaults to "us-west-2".

    Returns:
        file: The file object after writing the secret.

    Raises:
        ClientError: If there is an error retrieving the secret from AWS Secrets Manager.
    """
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    secret = get_secret_value_response['SecretString']
    with open(out_file, 'w') as f:
        f.write(secret)
    f.close()
    return f