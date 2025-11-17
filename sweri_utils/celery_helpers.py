from celery import group

from sweri_utils.celery_tasks import download_and_insert_service_chunk


def get_id_chunks(ids, chunk_size=500):
    if chunk_size == 1:
        for id in ids:
            yield [id]
        return

    for i in range(0, len(ids), chunk_size):
        yield ids[i:i + chunk_size]

def download_and_insert_service(ids, url, out_sr, chunk_size, schema, destination_table, ogr_db_string, out_fields):

    t = []

    for id_chunk in get_id_chunks(ids, chunk_size):
        t.append(download_and_insert_service_chunk.s(id_chunk, url, out_sr, chunk_size, schema, destination_table, ogr_db_string, out_fields))

    g = group(t)()
    g.get()