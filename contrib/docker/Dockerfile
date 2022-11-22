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

ARG DOCKER_PROXY=artifactory.adv.evoforge.org/docker-remote/
FROM ${DOCKER_PROXY}rockylinux:9

ARG GRADLE_VERSION=7.5.1 THRIFT_VERSION=0.17.0 MAVEN_VERSION=3.8.6
ARG uid
ARG gid

RUN yum -y groupinstall "Development Tools" && \
    yum install -y ant wget unzip java-11-openjdk-devel && \
    cd /tmp && \
    wget -O gradle.zip https://services.gradle.org/distributions/gradle-${GRADLE_VERSION}-bin.zip && \
    mkdir /opt/gradle && \
    unzip -d /opt/gradle gradle.zip && \
    wget -O thrift.tar.gz https://archive.apache.org/dist/thrift/${THRIFT_VERSION}/thrift-${THRIFT_VERSION}.tar.gz && \
    tar zxf thrift.tar.gz && \
    cd thrift-${THRIFT_VERSION} && \
    export JAVA_HOME=/usr/lib/jvm/java-11-openjdk && \
    PATH=/opt/gradle/gradle-${GRADLE_VERSION}/bin:${PATH} ./configure --without-kotlin --without-python --without-py3 && \
    make && \
    make install && \
    cd / && \
    wget -O maven.tar.gz https://dlcdn.apache.org/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz && \
    mkdir /opt/maven && \
    tar zxf maven.tar.gz -C /opt/maven && \
    rm -f gradle.zip thrift.tar.gz maven.tar.gz && \
    yum clean all && \
    rm -rf /var/cache/yum && \
    groupadd -r -g ${gid} builder && \
    useradd -d /home/builder -l -m -r -g builder -u ${uid} builder -s /bin/bash

ENV MAVEN_HOME=/opt/maven/apache-maven-${MAVEN_VERSION}
ENV JAVA_HOME=/etc/alternatives/java_sdk_11_openjdk
ENV PATH=$MAVEN_HOME/bin:$PATH
USER builder

