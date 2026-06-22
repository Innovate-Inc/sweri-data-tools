#!/bin/bash

# Load docker image name from .env located in the docker directory
export $(grep DOCKER_IMAGE_NAME= ../.env | xargs)

# Run docker using the vars
docker run --pull always --network host --env-file=../.env $DOCKER_IMAGE_NAME python daily_progression.py
