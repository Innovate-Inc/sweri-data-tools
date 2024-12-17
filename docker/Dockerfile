# Use the official Python 3.11 image as the base image



FROM --platform=linux/amd64 postgres:15-bullseye

LABEL maintainer="PostGIS Project - https://postgis.net" \
      org.opencontainers.image.description="PostGIS 3.5.0+dfsg-1.pgdg110+1 spatial database extension with PostgreSQL 12 bullseye" \
      org.opencontainers.image.source="https://github.com/postgis/docker-postgis"

ENV POSTGIS_MAJOR 3
ENV POSTGIS_VERSION 3.5.0+dfsg-1.pgdg110+1

RUN apt-get update  \
      && apt-get install -y gdal-bin libgdal-dev \
      && apt-cache showpkg postgresql-$PG_MAJOR-postgis-$POSTGIS_MAJOR \
      && apt-get install -y --no-install-recommends \
           # ca-certificates: for accessing remote raster files;
           #   fix: https://github.com/postgis/docker-postgis/issues/307
           ca-certificates \
           \
           postgresql-$PG_MAJOR-postgis-$POSTGIS_MAJOR=$POSTGIS_VERSION \
           postgresql-$PG_MAJOR-postgis-$POSTGIS_MAJOR-scripts \
      && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /docker-entrypoint-initdb.d

COPY ./initdb-postgis.sh /docker-entrypoint-initdb.d/10_postgis.sh
COPY ./update-postgis.sh /usr/local/bin

FROM --platform=linux/amd64 python:3.11-bullseye

# Set environment variables to avoid interactive prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive


# Set the working directory
WORKDIR /app

# Copy the requirements file
COPY package-list.txt .

RUN apt-get update \
    && apt-get install -y binutils libproj-dev gdal-bin \
    && python3 -m pip install -r package-list.txt --no-cache-dir


COPY . .

ENTRYPOINT ["python"]
# Set the default command to run PostgreSQL
CMD ["python", "intersections-postgis.py"]
