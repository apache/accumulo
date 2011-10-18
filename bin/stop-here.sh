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

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

ACCUMULO="$ACCUMULO_HOME/.*/accumulo-start.*.jar"

if grep -q localhost $ACCUMULO_HOME/conf/slaves
then
    $bin/accumulo admin stop localhost
else
    for host in `hostname -a`
    do
       if grep -q $host $ACCUMULO_HOME/conf/slaves
       then
          $bin/accumulo admin stop $host
       fi
    done
fi

for signal in TERM KILL
do
    for svc in tserver gc master monitor logger tracer
    do
	PID=`ps -ef | egrep ${ACCUMULO} | grep "Main $svc" | grep -v grep | grep -v stop-here.sh | awk {'print \$2'} | head -1`
	if [ ! -z $PID ]; then
	    echo "stopping $svc on `hostname -a | head -1` with signal $signal"
	    kill -s ${signal} ${PID}
	fi
    done
done
