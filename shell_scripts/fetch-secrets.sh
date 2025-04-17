#!/bin/bash
cd ~
source source data-processing/bin/activate
parent_path=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )
cd "$parent_path"
python ../fetch_secrets.py
