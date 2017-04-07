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
# Ref: http://stackoverflow.com/questions/59895/
SOURCE="${BASH_SOURCE[0]}"
while [ -h "${SOURCE}" ]; do # resolve $SOURCE until the file is no longer a symlink
   DIR=$( cd -P "$( dirname "${SOURCE}" )" && pwd )
   SOURCE=$(readlink "${SOURCE}")
   [[ "${SOURCE}" != /* ]] && SOURCE="${DIR}/${SOURCE}" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
DIR=$( cd -P "$( dirname "${SOURCE}" )" && pwd )
# Stop: Resolve Script Directory
LOG_DIR=${DIR}/logs
mkdir -p "$LOG_DIR"

# Source environment
. "${DIR}/stress-env.sh"

ts=$(date +%Y%m%d%H%M%S)
host=$(hostname)
# We want USERPASS to word split
"${ACCUMULO_HOME}/bin/accumulo org.apache.accumulo.test.stress.random.Scan" "$INSTANCE" $USERPASS "$SCAN_SEED" "$CONTINUOUS_SCAN" "$SCAN_BATCH_SIZE" \
    > "$LOG_DIR/${ts}_${host}_reader.out" \
    2> "$LOG_DIR/${ts}_${host}_reader.err"
