#!/bin/bash

parent_path=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )
cd "$parent_path"
cd ../docker
docker compose -f docker-compose-daily-progression.yml up -d
