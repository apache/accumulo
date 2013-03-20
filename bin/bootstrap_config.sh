#! /usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Start: Resolve Script Directory
SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
   bin="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
   SOURCE="$(readlink "$SOURCE")"
   [[ $SOURCE != /* ]] && SOURCE="$bin/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
bin="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
# Stop: Resolve Script Directory

#
# Resolve accumulo home for bootstrapping
#
ACCUMULO_HOME=$( cd -P ${bin}/.. && pwd )

SIZE=""
TYPE=""
if [ -n "$1" ]; then
   SIZE=$1
   TYPE="standalone"
fi

if [ "$2" = "native" ]; then
   TYPE="native-standalone"
fi

if [ -z "${SIZE}" -a -z "${TYPE}" ]; then
   echo "Please choose from the following example configurations..."
   echo ""
fi

if [ -z "${SIZE}" ]; then
   echo "Choose the heap configuration:"
   select DIRNAME in $(cd ${ACCUMULO_HOME}/conf/examples && ls -d */standalone/.. | sed -e 's/\/.*//'); do
      echo "Using '${DIRNAME}' configuration"
      SIZE=${DIRNAME}
      break
   done
fi

if [ -z "${TYPE}" ]; then
   echo
   echo "Choose the Accumulo memory-map type:"
   select TYPENAME in Java Native; do
      if [ "${TYPENAME}" = "native" ]; then
         TYPE="native-standalone"
      else
         TYPE="standalone"
      fi
      echo "Using '${TYPE}' configuration"
      echo
      break
   done
fi

CONF_DIR="${ACCUMULO_HOME}/conf/examples/${SIZE}/${TYPE}"

echo "Initializing default configuration from '${CONF_DIR}'"
#
# Perform a copy with update here to not overwrite existing files
#
cp -vu ${CONF_DIR}/* ${ACCUMULO_HOME}/conf
