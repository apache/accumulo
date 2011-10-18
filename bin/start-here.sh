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
# This script starts all the accumulo services on this host
#

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

HOSTS="`hostname -a` `hostname` localhost"
for host in $HOSTS
do
    if grep -q "^${host}\$" $ACCUMULO_HOME/conf/slaves
    then
       ${bin}/start-server.sh $host logger
       ${bin}/start-server.sh $host tserver "tablet server"
       break
    fi
done

for host in $HOSTS
do
    if grep -q "^${host}\$" $ACCUMULO_HOME/conf/masters
    then
       ${bin}/accumulo org.apache.accumulo.server.master.state.SetGoalState NORMAL
       ${bin}/start-server.sh $host master
       break
    fi
done

for host in $HOSTS
do
    if [ ${host} = ${GC} ]
    then
	${bin}/start-server.sh $GC gc "garbage collector"
	break
    fi
done

for host in $HOSTS
do
    if [ ${host} = ${MONITOR} ]
    then
	${bin}/start-server.sh $MONITOR monitor 
	break
    fi
done

for host in $HOSTS
do
    if grep -q "^${host}\$" $ACCUMULO_HOME/conf/tracers
    then
	${bin}/start-server.sh $host tracer 
	break
    fi
done
