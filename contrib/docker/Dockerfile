#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

ARG DOCKER_PROXY=""
FROM ${DOCKER_PROXY}rockylinux:9

ARG THRIFT_VERSION=0.17.0

ADD entrypoint.sh /

RUN yum -y groupinstall "Development Tools" && \
    yum install -y sudo wget maven java-11-openjdk-devel && \
    cd /tmp && \
    wget -O thrift.tar.gz https://archive.apache.org/dist/thrift/${THRIFT_VERSION}/thrift-${THRIFT_VERSION}.tar.gz && \
    tar zxf thrift.tar.gz && \
    cd thrift-${THRIFT_VERSION} && \
    export JAVA_HOME=/usr/lib/jvm/java-11-openjdk && \
    ./configure --without-libs --without-kotlin --without-python --without-py3 && \
    make && \
    make install && \
    yum clean all && \
    rm -rf /var/cache/yum && \
    groupadd -r builder && \
    useradd -d /home/builder -l -m -r -g builder builder -s /bin/bash

ENV JAVA_HOME=/etc/alternatives/java_sdk_11_openjdk

ENTRYPOINT ["/entrypoint.sh"]
