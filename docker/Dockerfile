FROM ghcr.io/osgeo/gdal:ubuntu-full-3.11.0

# Set environment variables to avoid interactive prompts during package installation
#ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y python3-pip python3-venv git
#RUN apt-get install -y binutils python3-dev gdal-bin libpq5 libpq-dev libproj-dev libgdal-dev python3-gdal
#RUN export CPLUS_INCLUDE_PATH=/usr/include/gdal
#RUN export C_INCLUDE_PATH=/usr/include/gdal
RUN python -m venv --system-site-packages /venv
ENV PATH="/venv/bin:${PATH}"

# Set the working directory
WORKDIR /src

# Copy the requirements file
COPY . .

RUN pip install -r docker/requirements.txt --no-cache-dir

