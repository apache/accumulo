#! /bin/bash
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

VERSION="1"
IMAGE="accumulo-build-environment-${VERSION}"
M2_DIR="${HOME}/.m2"

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
cd $SCRIPT_DIR

# Build the image if needed
if [[ $(docker images -q $IMAGE) == "" ]]; then
  cd docker
  docker build --build-arg uid=$(id -u ${USER}) --build-arg gid=$(id -g ${USER}) -t $IMAGE .
  cd $SCRIPT_DIR
fi

# Need absolute paths for Docker volume mounts
cd ..
SOURCE_DIR=`pwd`
cd $SCRIPT_DIR

docker run --rm -v $M2_DIR:/home/builder/.m2 -v $SOURCE_DIR:/SOURCES $IMAGE /bin/bash -c 'cd /SOURCES && rm -rf core/src/main/thrift-gen-java && mvn -Pthrift generate-sources && mvn clean package'
