#  Licensed to the Apache Software Foundation (ASF) under one   *
#  or more contributor license agreements.  See the NOTICE file *
#  distributed with this work for additional information        *
#  regarding copyright ownership.  The ASF licenses this file   *
#  to you under the Apache License, Version 2.0 (the            *
#  "License"); you may not use this file except in compliance   *
#  with the License.  You may obtain a copy of the License at   *
#                                                               *
#    http://www.apache.org/licenses/LICENSE-2.0                 *
#                                                               *
#  Unless required by applicable law or agreed to in writing,   *
#  software distributed under the License is distributed on an  *
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY       *
#  KIND, either express or implied.  See the License for the    *
#  specific language governing permissions and limitations      *
#  under the License.                                           *

FROM ubuntu:16.04

ENV DEBIAN_FRONTEND=noninteractive

# install deps
RUN apt-get update -y && apt-get install -y \
        wget \
        python-dev \
        python-pip \
        libczmq-dev \
        libcurlpp-dev \
        curl \
        libssl-dev \
        git \
        inetutils-telnet \
        bind9utils \
        zip \
        locales \
        tzdata \
        unzip \
    && apt-get clean

RUN dpkg-reconfigure tzdata && \
    sed -i -e 's/# en_CA.UTF-8 UTF-8/en_CA.UTF-8 UTF-8/' /etc/locale.gen && \
    sed -i -e 's/# en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' /etc/locale.gen && \
    echo 'LANG="en_CA.UTF-8"' > /etc/default/locale && \
    dpkg-reconfigure locales && \
    update-locale LANG=en_CA.UTF-8

ENV LANG=en_CA.UTF-8

RUN pip install --upgrade pip

COPY requirements.txt ./

# Since we install vanilla Airflow, we also want to have support for Postgres and Kubernetes
RUN pip install -r requirements.txt

# install airflow
COPY airflow.tar.gz /tmp/airflow.tar.gz
RUN pip install /tmp/airflow.tar.gz

COPY airflow-env-init.sh /tmp/airflow-env-init.sh

COPY bootstrap.sh /bootstrap.sh
RUN chmod +x /bootstrap.sh

ENTRYPOINT ["/bootstrap.sh"]
