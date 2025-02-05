from .s3 import upload_to_s3
from .sql import pg_copy_to_csv


def array_to_dict(keys, values):
    """
    shallow array to dictionary converter
    :param keys: names of keys to assign in new object
    :param values: list of values in same order as keys
    :return: new dict
    """
    x = dict()
    for i, f in enumerate(keys):
        x[f] = values[i]
    return x


def create_csv_and_upload_to_s3(cursor, schema, table, columns, filename, bucket):
    f = pg_copy_to_csv(cursor, schema, table, filename, columns)
    return upload_to_s3(bucket, f.name, filename)