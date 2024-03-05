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

FROM continuumio/miniconda3:4.8.2-alpine

USER root

ENV PATH="/opt/conda/bin:$PATH"

RUN apk update --no-cache && apk add --no-cache --virtual .build-deps git openjdk8

COPY VERSION.txt /VERSION.txt
COPY common /common
COPY granule_ingester/granule_ingester /sdap/granule_ingester
COPY granule_ingester/setup.py /sdap/setup.py
COPY granule_ingester/requirements.txt /sdap/requirements.txt
COPY granule_ingester/conda-requirements.txt /sdap/conda-requirements.txt
COPY granule_ingester/docker/install_nexusproto.sh /install_nexusproto.sh
COPY granule_ingester/docker/entrypoint.sh /entrypoint.sh

ARG BUILD_NEXUSPROTO
ARG APACHE_NEXUSPROTO=https://github.com/apache/incubator-sdap-nexusproto.git
ARG APACHE_NEXUSPROTO_BRANCH=master

RUN ./install_nexusproto.sh $APACHE_NEXUSPROTO $APACHE_NEXUSPROTO_BRANCH
RUN cd /common && python setup.py install
RUN cd /sdap && python setup.py install

RUN pip install boto3==1.16.10

RUN apk del .build-deps

ENTRYPOINT ["/bin/sh", "/entrypoint.sh"]
