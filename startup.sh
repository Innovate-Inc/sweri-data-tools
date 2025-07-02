#!/bin/bash

while ! systemctl is-active --quiet arcgisserver; do
  sleep 2
done


URL="https://localhost:6443/arcgis/rest/info"
until curl --silent --show-error "$URL"; do
  sleep 2
done

echo "hostname=$(hostname)" > /opt/arcgis/server/framework/etc/hostname.properties

sh /opt/arcgis/server/tools/joinsite -f /home/arcgis/connection.json
