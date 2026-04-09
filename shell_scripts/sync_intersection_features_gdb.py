#!/usr/bin/env python3
"""
sync_intersection_features_gdb.py

Hourly sync script for EC2 instances running ArcGIS Server.
Downloads the latest intersection_features File Geodatabase from the private
S3 bucket, extracts it to the configured local path, and replaces the previous copy.

Schedule with cron (runs every hour):
    0 * * * * /path/to/conda/envs/sweri-python/bin/python /path/to/scripts/shell_scripts/sync_intersection_features_gdb.py >> /var/log/sync_intersection_features_gdb.log 2>&1

Required environment variables (set in /etc/environment or a sourced .env):
    INTERSECTION_FEATURES_GDB_BUCKET   - private S3 bucket name
    INTERSECTION_FEATURES_GDB_S3_OBJ   - S3 object key of the zipped GDB
                                         (default: intersection_features/intersection_features.zip)
    INTERSECTION_FEATURES_GDB_LOCAL_DIR - local directory where the GDB should be extracted
                                         (e.g. /arcgisserver/directories/arcgissystem/intersection_features)
    AWS_DEFAULT_REGION                 - AWS region (default: us-west-2)
    AWS_SSO_PROFILE_NAME               - (optional) AWS SSO profile name
"""

import logging
import os
import shutil
import tempfile
import zipfile
from datetime import datetime
from pathlib import Path

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

try:
    import arcpy
    ARCPY_AVAILABLE = True
except ModuleNotFoundError:
    ARCPY_AVAILABLE = False
    logging.warning('arcpy not available — Repair Geometry step will be skipped')

load_dotenv()

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def download_and_extract_gdb(bucket: str, s3_obj: str, local_dir: str) -> None:
    """
    Downloads the zipped GDB from S3, extracts it, runs Repair Geometry on
    feat_source = 'treatments', then atomically replaces the live GDB.

    :param bucket: S3 bucket name
    :param s3_obj: S3 object key for the zipped GDB
    :param local_dir: Local directory to extract the GDB into
    """
    local_dir_path = Path(local_dir)
    local_dir_path.mkdir(parents=True, exist_ok=True)

    gdb_name = Path(s3_obj).stem  # e.g. 'intersection_features'
    final_gdb_path = local_dir_path / f"{gdb_name}.gdb"
    tmp_extract_dir = local_dir_path / f"{gdb_name}_tmp_{datetime.now().strftime('%Y%m%d%H%M%S')}"

    s3 = boto3.client('s3')

    with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as tmp_zip:
        tmp_zip_path = tmp_zip.name

    try:
        logger.info(f'Downloading s3://{bucket}/{s3_obj} → {tmp_zip_path}')
        s3.download_file(bucket, s3_obj, tmp_zip_path)
        logger.info('Download complete')

        logger.info(f'Extracting {tmp_zip_path} → {tmp_extract_dir}')
        tmp_extract_dir.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(tmp_zip_path, 'r') as zf:
            zf.extractall(tmp_extract_dir)

        # Find the .gdb directory inside the extracted content
        extracted_gdbs = list(tmp_extract_dir.glob('*.gdb'))
        if not extracted_gdbs:
            raise FileNotFoundError(f'No .gdb directory found after extracting {tmp_zip_path}')
        extracted_gdb = extracted_gdbs[0]

        # run Repair Geometry before going live
        arcpy.management.RepairGeometry(os.path.join(extracted_gdb, 'intersection_features'), 'DELETE_NULL', 'ESRI')

        # Atomically replace the old GDB: rename old → backup, new → final, remove backup
        backup_path = local_dir_path / f"{gdb_name}.gdb.bak"
        if final_gdb_path.exists():
            if backup_path.exists():
                shutil.rmtree(backup_path)
            final_gdb_path.rename(backup_path)
            logger.info(f'Previous GDB backed up to {backup_path}')

        shutil.move(str(extracted_gdb), str(final_gdb_path))
        logger.info(f'GDB updated at {final_gdb_path}')

        # Remove backup only after successful update
        if backup_path.exists():
            shutil.rmtree(backup_path)

    except ClientError as e:
        logger.error(f'S3 download failed: {e}')
        raise
    except Exception as e:
        logger.error(f'GDB sync failed: {e}')
        raise
    finally:
        if os.path.exists(tmp_zip_path):
            os.remove(tmp_zip_path)
        if tmp_extract_dir.exists():
            shutil.rmtree(tmp_extract_dir, ignore_errors=True)


def main():
    bucket = os.getenv('INTERSECTION_FEATURES_GDB_BUCKET')
    s3_obj = os.getenv('INTERSECTION_FEATURES_GDB_S3_OBJ')
    local_dir = os.getenv('INTERSECTION_FEATURES_GDB_LOCAL_DIR')

    if not bucket:
        logger.error('INTERSECTION_FEATURES_GDB_BUCKET is not set. Exiting.')
        raise SystemExit(1)
    if not local_dir:
        logger.error('INTERSECTION_FEATURES_GDB_LOCAL_DIR is not set. Exiting.')
        raise SystemExit(1)

    start = datetime.now()
    logger.info(f'Starting intersection_features GDB sync at {start.isoformat()}')
    download_and_extract_gdb(bucket, s3_obj, local_dir)
    elapsed = datetime.now() - start
    logger.info(f'Sync completed in {elapsed}')


if __name__ == '__main__':
    main()

