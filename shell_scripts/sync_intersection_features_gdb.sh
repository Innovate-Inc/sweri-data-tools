#!/bin/bash
# sync_intersection_features_gdb.sh
#
# Shell wrapper for the intersection_features GDB hourly sync script.
# Designed to be called by cron on the EC2 instance running ArcGIS Server.
#
# Crontab entry (runs every hour at :00):
#   0 * * * * /path/to/scripts/shell_scripts/sync_intersection_features_gdb.sh >> /var/log/sync_intersection_features_gdb.log 2>&1
#
# Environment variables expected (set these in /etc/environment or a sourced file):
#   INTERSECTION_FEATURES_GDB_BUCKET
#   INTERSECTION_FEATURES_GDB_S3_OBJ   (optional, defaults in python script)
#   INTERSECTION_FEATURES_GDB_LOCAL_DIR
#   AWS_DEFAULT_REGION
#   AWS_SSO_PROFILE_NAME               (optional)

# Source .bashrc to get ARCGISHOME and other configuration
if [ -f ~/.bashrc ]; then
    . ~/.bashrc
else
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] WARNING: ~/.bashrc not found"
fi

# Initialize conda for non-interactive shell and activate the environment
# This works in both interactive and non-interactive (cron) contexts
if ! eval "$(conda shell.bash hook)"; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: Failed to initialize conda"
    exit 1
fi

if ! conda activate sweri-python; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: Failed to activate conda environment"
    exit 1
fi

# Navigate to the scripts directory (parent of shell_scripts)
parent_path=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )
cd "$parent_path"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting intersection_features GDB sync"
python sync_intersection_features_gdb.py
EXIT_CODE=$?

if [ $EXIT_CODE -ne 0 ]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: GDB sync failed with exit code $EXIT_CODE"
else
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] GDB sync completed successfully"
fi

exit $EXIT_CODE

