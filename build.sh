#!/bin/bash

cp -f ./pygeoapi/api.py ../lib/python3.9/site-packages/pygeoapi-0.14.dev0-py3.9.egg/pygeoapi
cp -f ./pygeoapi/flask_app.py ../lib/python3.9/site-packages/pygeoapi-0.14.dev0-py3.9.egg/pygeoapi
cp -f ./pygeoapi/provider/postgresql.py ../lib/python3.9/site-packages/pygeoapi-0.14.dev0-py3.9.egg/pygeoapi/provider
cp -f ./pygeoapi/models/process_data.py ../lib/python3.9/site-packages/pygeoapi-0.14.dev0-py3.9.egg/pygeoapi/models

mkdir -p log

ip4=$(/sbin/ip -o -4 addr list eth0 | awk '{print $4}' | cut -d/ -f1)
sed -i "0,/url.*/{s/url.*/url: http:\/\/$ip4:8085/}" example-config.yml

cur=$PWD
sed -i "0,/.*ogc_schemas_location.*/{s/.*ogc_schemas_location.*/        ogc_schemas_location: ${cur//\//\\/}\/schema/}" example-config.yml
sed -i "0,/.*logfile.*/{s/.*logfile.*/    logfile: ${cur//\//\\/}\/log\/pygeoapi.log/}" example-config.yml
