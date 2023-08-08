#!/bin/bash

set +e

export PYGEOAPI_HOME=/pygeoapi
export PYGEOAPI_CONFIG="${PYGEOAPI_HOME}/example-config.yml"
export PYGEOAPI_OPENAPI="${PYGEOAPI_HOME}/example-openapi.yml"

pygeoapi serve