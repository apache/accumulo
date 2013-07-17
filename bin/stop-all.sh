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

. "$bin"/config.sh

echo "Stopping accumulo services..."
${bin}/accumulo admin "$@" stopAll

if [ $? -ne 0 ]; then
   echo "Invalid password or unable to connect to the master"
   echo "Press Ctrl-C to cancel now, or force shutdown in 15 seconds"
   sleep 10
else
   echo "Accumulo shut down cleanly"
fi

echo "Utilities and unresponsive servers will be shut down in 5 seconds"
sleep 5

#look for master and gc processes not killed by 'admin stopAll'
for signal in TERM KILL ; do
   for master in `grep -v '^#' "$ACCUMULO_CONF_DIR/masters"`; do
      ${bin}/stop-server.sh $master "$ACCUMULO_HOME/lib/accumulo-start.jar" master $signal
   done

   ${bin}/stop-server.sh "$GC" "$ACCUMULO_HOME/lib/accumulo-start.jar" gc $signal

   ${bin}/stop-server.sh "$MONITOR" "$ACCUMULO_HOME/.*/accumulo-start.*.jar" monitor $signal

   for tracer in `egrep -v '(^#|^\s*$)' "$ACCUMULO_CONF_DIR/tracers"`; do
      ${bin}/stop-server.sh $tracer "$ACCUMULO_HOME/.*/accumulo-start.*.jar" tracer $signal
   done
done

# stop tserver still running
${bin}/tdown.sh

echo "Cleaning all server entries in ZooKeeper"
$ACCUMULO_HOME/bin/accumulo org.apache.accumulo.server.util.ZooZap -master -tservers -tracers

