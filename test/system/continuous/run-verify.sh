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

. "${bin}/mapred-setup.sh"

AUTH_OPT="";
[[ -n $VERIFY_AUTHS ]] && AUTH_OPT="--auths $VERIFY_AUTHS"

SCAN_OPT=--offline
[[ $SCAN_OFFLINE == false ]] && SCAN_OPT=

"$ACCUMULO_HOME/bin/tool.sh" "$SERVER_LIBJAR" org.apache.accumulo.test.continuous.ContinuousVerify -Dmapreduce.job.reduce.slowstart.completedmaps=0.95 -libjars "$SERVER_LIBJAR" "$AUTH_OPT" -i "$INSTANCE_NAME" -z "$ZOO_KEEPERS" -u "$USER" -p "$PASS" --table "$TABLE" --output "$VERIFY_OUT" --maxMappers "$VERIFY_MAX_MAPS" --reducers "$VERIFY_REDUCERS" "$SCAN_OPT"
