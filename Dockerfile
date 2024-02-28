FROM mobilitydb/mobilitydb:14-3.2-1

LABEL maintainer="Taehoon Kim <kim.taehoon@aist.go.jp>"

# Docker file for full geoapi server with libs/packages for all providers.
# Server runs with gunicorn. You can override ENV settings.
# Defaults:
# SCRIPT_NAME=/
# CONTAINER_NAME=pygeoapi
# CONTAINER_HOST=0.0.0.0
# CONTAINER_PORT=80
# WSGI_WORKERS=4
# WSGI_WORKER_TIMEOUT=6000
# WSGI_WORKER_CLASS=gevent

# Calls entrypoint.sh to run. Inspect it for options.
# Contains some test data. Also allows you to verify by running all unit tests.
# Simply run: docker run -it geopython/pygeoapi test
# Override the default config file /pygeoapi/local.config.yml
# via Docker Volume mapping or within a docker-compose.yml file. See example at
# https://github.com/geopython/demo.pygeoapi.io/tree/master/services/pygeoapi

# Build arguments
# add "--build-arg BUILD_DEV_IMAGE=true" to Docker build command when building with test/doc tools

# ARGS
ARG TZ="Etc/UTC+9"
ARG LANG="en_US.UTF-8"
ARG BUILD_DEV_IMAGE="false"

# ENV settings
ENV TZ=${TZ} \
    LANG=${LANG} \
    DEBIAN_FRONTEND="noninteractive" \
    DEB_BUILD_DEPS="\
      curl \
      python3-dev \
      python3-pip \
      python3-setuptools \
      python3-wheel \
      python3-yaml \
      python3-software-properties \
      locales \
      locales-all \
      software-properties-common \
      unzip"

# Install operating system dependencies
RUN apt-get update -y \
    && apt-get install -y --fix-missing --no-install-recommends ${DEB_BUILD_DEPS} \
    && update-locale LANG=${LANG}

RUN rm /docker-entrypoint-initdb.d/mobilitydb.sh
COPY sql/initdb-mobilitydb.sh /docker-entrypoint-initdb.d/mobilitydb.sh
RUN chmod +x /docker-entrypoint-initdb.d/mobilitydb.sh

WORKDIR /pygeoapi
RUN mkdir -p /pygeoapi/pygeoapi

# Add files required for pip/setuptools
ADD requirements*.txt setup.py README.md /pygeoapi/
ADD pygeoapi/__init__.py /pygeoapi/pygeoapi/

## Install pygeoapi
RUN \
    if [ "$BUILD_DEV_IMAGE" = "true" ] ; then pip3 install -r requirements-dev.txt; fi \
    && pip3 install -r requirements-provider.txt \
    && pip3 install -e .
#
##RUN \
##    # Cleanup TODO: remove unused Locales and TZs
##    apt-get remove --purge -y ${DEB_BUILD_DEPS} \
##    && apt autoremove -y  \
##    && rm -rf /var/lib/apt/lists/*

ADD . /pygeoapi
RUN python3 setup-mf-api.py install
RUN chmod +x /pygeoapi/build.sh
RUN ./build.sh
RUN chmod +x /pygeoapi/run.sh