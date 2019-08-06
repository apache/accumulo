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
while [[ -h "${SOURCE}" ]]; do # resolve $SOURCE until the file is no longer a symlink
   bin=$( cd -P "$( dirname "${SOURCE}" )" && pwd )
   SOURCE=$(readlink "${SOURCE}")
   [[ "${SOURCE}" != /* ]] && SOURCE="${bin}/${SOURCE}" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
bin=$( cd -P "$( dirname "${SOURCE}" )" && pwd )
script=$( basename "${SOURCE}" )
# Stop: Resolve Script Directory

CONTINUOUS_CONF_DIR=${CONTINUOUS_CONF_DIR:-${bin}}
. "$CONTINUOUS_CONF_DIR/continuous-env.sh"

SERVER_LIBJAR="$ACCUMULO_HOME/lib/accumulo-test.jar"

VIS_OPT=''
[[ -n $VISIBILITIES ]] && VIS_OPT="--visibilities \"$VISIBILITIES\""

CHECKSUM_OPT='--addCheckSum'
[[ $CHECKSUM == false ]] && CHECKSUM_OPT=''

"$ACCUMULO_HOME/bin/tool.sh" "$SERVER_LIBJAR" org.apache.accumulo.test.continuous.BulkIngest -Dmapreduce.job.reduce.slowstart.completedmaps=0.95 -libjars "$SERVER_LIBJAR" "$VIS_OPT" -i $INSTANCE_NAME -z $ZOO_KEEPERS -u $USER -p $PASS --table $TABLE --dir $BULK_DIR --mapTasks $BULK_MAP_TASKS --mapNodes $BULK_MAP_NODES --min $MIN --max $MAX --maxColF $MAX_CF --maxColQ $MAX_CQ $CHECKSUM_OPT
