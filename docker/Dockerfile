FROM python:3.11-bookworm

# Set environment variables to avoid interactive prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive
# Set the working directory
WORKDIR /code

# Copy the requirements file
COPY . .
RUN apt-get update && apt-get install -y software-properties-common
RUN apt-get install -y binutils python3-dev gdal-bin libpq5 libpq-dev libproj-dev libgdal-dev python3-gdal
RUN export CPLUS_INCLUDE_PATH=/usr/include/gdal
RUN export C_INCLUDE_PATH=/usr/include/gdal
RUN python3 -m pip install -r docker/requirements.txt --no-cache-dir

