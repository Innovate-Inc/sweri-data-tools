import os

from sweri_utils.sql import connect_to_pg_db


def create_db_conn_from_envs(k):
    docker_db_host = os.getenv(f'{k}_DB_HOST')
    docker_db_port = int(os.getenv(f'{k}_DB_PORT'))
    docker_db_name = os.getenv(f'{k}_DB_NAME')
    docker_db_user = os.getenv(f'{k}_DB_USER')
    docker_db_password = os.getenv(f'{k}_DB_PASSWORD')
    return connect_to_pg_db(docker_db_host, docker_db_port, docker_db_name, docker_db_user, docker_db_password)
