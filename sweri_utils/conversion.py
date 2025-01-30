from sweri_utils.s3 import upload_to_s3


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


def pg_copy_to_csv(cursor, schema, table, filename):
    with open(f'{filename}.csv', 'w') as f:
        cursor.copy_expert(f'COPY {schema}.{table} TO STDOUT WITH CSV HEADER', f)
    return f

def create_csv_and_upload_to_s3(cursor, schema, table, filename, bucket, key):
    f = pg_copy_to_csv(cursor, schema, table, filename)
    return upload_to_s3(bucket, f)