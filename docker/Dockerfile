FROM ghcr.io/osgeo/gdal:ubuntu-full-3.11.0

# Set environment variables to avoid interactive prompts during package installation
#ENV DEBIAN_FRONTEND=noninteractive

# Temporary fix: remove the Apache Arrow apt source injected by the GDAL base image
# whose signing key (9E922B2D60E9FD1C) is no longer valid, causing apt-get update to fail.
RUN rm -f /etc/apt/sources.list.d/apache-arrow.list /etc/apt/sources.list.d/*arrow* \
          /usr/share/keyrings/apache-arrow* /etc/apt/trusted.gpg.d/apache-arrow*
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

