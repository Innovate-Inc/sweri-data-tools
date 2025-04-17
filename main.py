import logging
import os
import sys
from datetime import datetime

from dotenv import load_dotenv

from intersections import run_intersections
from sweri_utils.s3 import fetch_secrets
from sweri_utils.sql import connect_to_pg_db

import watchtower

logger = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', encoding='utf-8', level=logging.INFO,
                    datefmt='%Y-%m-%d %H:%M:%S')
logger.addHandler(watchtower.CloudWatchLogHandler())
logger.setLevel(logging.INFO)

if __name__ == "__main__":
    if len(sys.argv) < 5:
        print("missing args")
        sys.exit(1)

    fetch_secrets(secret_name=sys.argv[1], out_file=sys.argv[2])
    fetch_secrets(secret_name=sys.argv[3], out_file=sys.argv[4])

    logger.info('starting intersection processing')
    load_dotenv('.env')
    script_start = datetime.now()
    sr_wkid = 3857

    # s3 bucket used for intersections
    s3_bucket_name = os.getenv('S3_BUCKET')
    # configure intersection sources
    intersection_src_url = os.getenv('INTERSECTION_SOURCES_URL')
    ############### database connections ################
    # local docker db environment variables
    docker_db_schema = os.getenv('DOCKER_DB_SCHEMA')
    docker_db_host = os.getenv('DOCKER_DB_HOST')
    docker_db_port = int(os.getenv('DOCKER_DB_PORT'))
    docker_db_name = os.getenv('DOCKER_DB_NAME')
    docker_db_user = os.getenv('DOCKER_DB_USER')
    docker_db_password = os.getenv('DOCKER_DB_PASSWORD')
    print(docker_db_schema)
    sys.exit(1)
    docker_pg_cursor, docker_pg_conn = connect_to_pg_db(docker_db_host, docker_db_port, docker_db_name, docker_db_user,
                                                        docker_db_password)

    # rds db params
    rds_db_schema = os.getenv('RDS_SCHEMA')
    rds_db_host = os.getenv('RDS_DB_HOST')
    rds_db_port = int(os.getenv('RDS_DB_PORT'))
    rds_db_name = os.getenv('RDS_DB_NAME')
    rds_db_user = os.getenv('RDS_DB_USER')
    rds_db_password = os.getenv('RDS_DB_PASSWORD')
    rds_pg_cursor, rds_pg_conn = connect_to_pg_db(rds_db_host, rds_db_port, rds_db_name, rds_db_user, rds_db_password)
    ############## intersections processing in docker ################
    # function that runs everything for creating new intersections in docker, uploading the results to s3, and swapping the tables on the rds instance
    run_intersections(docker_pg_cursor, docker_pg_conn, docker_db_schema, rds_pg_cursor, rds_pg_conn, rds_db_schema,
                      s3_bucket_name,
                      script_start, sr_wkid, intersection_src_url)
    logger.info(f'completed intersection processing, total runtime: {datetime.now() - script_start}')

