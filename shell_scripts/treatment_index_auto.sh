#!/bin/bash
export ARCGISHOME=/opt/arcgis/server
. ~/miniconda3/etc/profile.d/conda.sh
conda activate sweri-python
parent_path=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )
cd "$parent_path"
python ../treatment_index.py
