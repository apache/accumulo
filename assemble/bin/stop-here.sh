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

#
# This script safely stops all the accumulo services on this host
#

# Start: Resolve Script Directory
SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
   bin="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
   SOURCE="$(readlink "$SOURCE")"
   [[ $SOURCE != /* ]] && SOURCE="$bin/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
bin="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
# Stop: Resolve Script Directory

. "$bin"/config.sh

# Determine hostname without errors to user
HOSTS_TO_CHECK=($(hostname -a 2> /dev/null | head -1) $(hostname -f))

if egrep -q localhost\|127.0.0.1 "$ACCUMULO_CONF_DIR/tservers"; then
   "$bin/accumulo" admin stop localhost
else
   for host in "${HOSTS_TO_CHECK[@]}"; do
      if grep -q "$host" "$ACCUMULO_CONF_DIR"/tservers; then
         "${bin}/accumulo" admin stop "$host"
      fi
   done
fi

for HOSTNAME in "${HOSTS_TO_CHECK[@]}"; do
   for signal in TERM KILL; do
      for svc in tserver gc master monitor tracer; do
         "$ACCUMULO_HOME"/bin/stop-server.sh "$HOSTNAME" "$ACCUMULO_HOME/lib/accumulo-start.jar" $svc $signal
      done
   done
done
