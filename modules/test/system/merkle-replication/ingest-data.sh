#!/usr/bin/env bash

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
while [[ -h "${SOURCE}" ]]; do # resolve $SOURCE until the file is no longer a symlink
   dir=$( cd -P "$( dirname "${SOURCE}" )" && pwd )
   SOURCE=$(readlink "${SOURCE}")
   [[ "${SOURCE}" != /* ]] && SOURCE="${dir}/${SOURCE}" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
dir=$( cd -P "$( dirname "${SOURCE}" )" && pwd )
script=$( basename "${SOURCE}" )
# Stop: Resolve Script Directory

# Guess at ACCUMULO_HOME and ACCUMULO_CONF_DIR if not already defined
ACCUMULO_HOME=${ACCUMULO_HOME:-"${dir}/../../.."}
ACCUMULO_CONF_DIR=${ACCUMULO_CONF_DIR:-"$ACCUMULO_HOME/conf"}

# Get the configuration values
. ./merkle-env.sh

# Ingest data into the source table
$ACCUMULO_HOME/bin/accumulo org.apache.accumulo.test.replication.merkle.ingest.RandomWorkload --table $SOURCE_TABLE_NAME \
    -i $SOURCE_INSTANCE -z $SOURCE_ZOOKEEPERS -u $SOURCE_ACCUMULO_USER -p $SOURCE_ACCUMULO_PASSWORD -d $DELETE_PERCENT \
    -cf $MAX_CF -cq $MAX_CQ -r $MAX_ROW -n $NUM_RECORDS
