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


bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

${bin}/accumulo admin stopAll "$@"

if [ $? -ne 0 ]; then
echo 'Invalid password or unable to connect to the master'
echo 'Press Ctrl-C to cancel now, or force shutdown in 15 seconds'
sleep 10
else
echo 'Accumulo shut down cleanly'
fi

echo 'Utilities and unresponsive servers will be shut down in 5 seconds'
sleep 5

#look for master and gc processes not killed by 'admin stopAll'
for signal in TERM KILL
do
   for master in `grep -v '^#' "$ACCUMULO_HOME/conf/masters"`
   do
      ${bin}/stop-server.sh $master "$ACCUMULO_HOME/.*/accumulo-start.*.jar" master $signal
   done

   ${bin}/stop-server.sh $GC "$ACCUMULO_HOME/.*/accumulo-start.*.jar" gc $signal

   ${bin}/stop-server.sh $MONITOR "$ACCUMULO_HOME/.*/accumulo-start.*.jar" monitor $signal

   for tracer in `grep -v '^#' "$ACCUMULO_HOME/conf/tracers"`
   do
      ${bin}/stop-server.sh $tracer "$ACCUMULO_HOME/.*/accumulo-start.*.jar" tracer $signal
   done
done


# stop tserver/loggers still running
${bin}/tdown.sh

echo 'Cleaning all server entries in zookeeper'
$ACCUMULO_HOME/bin/accumulo org.apache.accumulo.server.util.ZooZap -master -tservers -loggers -tracers

