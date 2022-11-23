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

# Image name and version. The version needs to be incremented
# when the Dockerfile is changed
#
VERSION="1"
IMAGE="accumulo-build-environment:${VERSION}"

# User's local .m2 directory
M2_DIR="${HOME}/.m2"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
cd "$SCRIPT_DIR" || exit 1

# If the image does not exist locally, then we need to create it. The
# user's uid and gid are baked into the image so that we don't run
# into permission issues reading/writing from the filesystem during
# the build phase
#
if [[ $(docker images -q $IMAGE) == "" ]]; then
  cd docker || exit 1
  docker build -t $IMAGE .
  cd "$SCRIPT_DIR" || exit 1
fi

# Need absolute path to the accumulo source directory for Docker volume mounts
#
cd .. || exit 1
SOURCE_DIR=$(pwd)
cd "$SCRIPT_DIR" || exit 1

# Create a container from the image, mounting the user's local .m2 directory and the accumulo
# source code directory into the image, using Maven to run the build with the thrift profile.
# The build output is written to the local accumulo source code directory
#
docker run --rm \
  -e HOST_UID="$(id -u "${USER}")" \
  -e HOST_GID="$(id -g "${USER}")" \
  -v "$M2_DIR":/home/builder/.m2 \
  -v "$SOURCE_DIR":/SOURCES \
   $IMAGE bash -c 'cd /SOURCES && mvn clean package -Pthrift'
