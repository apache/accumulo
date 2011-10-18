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


# only stop if there's not one already running
if [ "$1" = "`hostname`" ]; then
	PID=`ps -ef | grep "$ACCUMULO_HOME" | egrep ${2} | grep "Main ${3}" | grep -v grep | grep -v ssh | grep -v stop-server.sh | awk {'print \$2'} | head -1`
else
	PID=`ssh -q -o 'ConnectTimeout 8' $1 "ps -ef | grep \"$ACCUMULO_HOME\" |  egrep '${2}' | grep 'Main ${3}' | grep -v grep | grep -v ssh | grep -v stop-server.sh" | awk {'print $2'} | head -1`
fi
if [ ! -z $PID ]; then
        echo "stopping ${3} on $1";
        ssh -q -o 'ConnectTimeout 8' $1 "kill -s ${4} ${PID} 2>/dev/null"
fi;
