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

HADOOP_CMD=$HADOOP_LOCATION/bin/hadoop

SLAVES=$ACCUMULO_HOME/conf/slaves

echo 'stopping unresponsive tablet servers (if any) ...'
for server in `cat $SLAVES | grep -v '^#' `; do
        # only start if there's not one already running
        $ACCUMULO_HOME/bin/stop-server.sh $server "$ACCUMULO_HOME/.*/accumulo-start.*.jar" tserver TERM & 
done

sleep 10

echo 'stopping unresponsive tablet servers hard (if any) ...'
for server in `cat $SLAVES | grep -v '^#' `; do
        # only start if there's not one already running
        $ACCUMULO_HOME/bin/stop-server.sh $server "$ACCUMULO_HOME/.*/accumulo-start.*.jar" tserver KILL & 
done

echo 'Cleaning tablet server entries from zookeeper'
$ACCUMULO_HOME/bin/accumulo org.apache.accumulo.server.util.ZooZap -tservers
