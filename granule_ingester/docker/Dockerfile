# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM eclipse-temurin:8

USER root

COPY --chmod=755 granule_ingester/docker/install_python.sh /install_python.sh

# Install python
RUN /install_python.sh
RUN ln -s /opt/python/3.8.17/bin/python3.8  /opt/python/3.8.17/bin/python

RUN apt-get update && \
    apt-get install --no-install-recommends -y git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV PATH="/opt/python/3.8.17/bin:$PATH"

COPY VERSION.txt /VERSION.txt
COPY common /common
COPY granule_ingester/granule_ingester /sdap/granule_ingester
COPY granule_ingester/docker/install_nexusproto.sh /sdap/install_nexusproto.sh
COPY granule_ingester/docker/entrypoint.sh /entrypoint.sh

COPY granule_ingester/README.md README LICENSE.txt NOTICE /sdap/

ENV PYTHONUNBUFFERED=1 \
    # prevents python creating .pyc files
    PYTHONDONTWRITEBYTECODE=1 \
    \
    # pip
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    \
    # poetry
    # https://python-poetry.org/docs/configuration/#using-environment-variables
    POETRY_VERSION=1.5.1 \
    # make poetry install to this location
    POETRY_HOME="/opt/poetry" \
    # make poetry create the virtual environment in the project's root
    # it gets named `.venv`
    POETRY_VIRTUALENVS_IN_PROJECT=true \
    # do not ask any interactive question
    POETRY_NO_INTERACTION=1 \
    PYSETUP_PATH="/opt/pysetup" \
    \
    VENV_PATH="/sdap/.venv" \
    POETRY_CACHE_DIR="/opt/.cache/poetry"

WORKDIR /sdap
COPY granule_ingester/poetry.lock granule_ingester/pyproject.toml ./

ENV PATH="$POETRY_HOME/bin:$VENV_PATH/bin:$PATH"

RUN curl -sSL https://install.python-poetry.org -o /tmp/install_poetry.py &&  \
    python /tmp/install_poetry.py && \
    poetry install --no-dev &&  \
    rm -rf $POETRY_CACHE_DIR && \
    python /tmp/install_poetry.py --uninstall && \
    rm /tmp/install_poetry.py

ARG BUILD_NEXUSPROTO
ARG APACHE_NEXUSPROTO=https://github.com/apache/incubator-sdap-nexusproto.git
ARG APACHE_NEXUSPROTO_BRANCH=master

RUN ./install_nexusproto.sh $APACHE_NEXUSPROTO $APACHE_NEXUSPROTO_BRANCH
RUN cd /common && python setup.py install

RUN pip install boto3==1.16.10

ENTRYPOINT ["/bin/bash", "/entrypoint.sh"]
