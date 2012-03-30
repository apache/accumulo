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

HOST=$1
host $1 >/dev/null 2>/dev/null
if [ $? -ne 0 ]
then
  LOGHOST=$1
else
  LOGHOST="`host $1 | head -1 | cut -d' ' -f1`"
fi
SERVICE="$2"
LONGNAME="$3"
if [ -z "$LONGNAME" ] 
then
   LONGNAME="$2"
fi
SLAVES=`wc -l < ${ACCUMULO_HOME}/conf/slaves`

if [ $HOST == localhost -o $HOST == "`hostname`" ] 
then
  PID=`ps -ef | egrep ${ACCUMULO_HOME}/.*/accumulo.*.jar | grep "Main $SERVICE" | grep -v grep | awk {'print $2'} | head -1`
else
  PID=`$SSH $HOST ps -ef | egrep ${ACCUMULO_HOME}/.*/accumulo.*.jar | grep "Main $SERVICE" | grep -v grep | awk {'print $2'} | head -1`
fi

if [ -z $PID ]; then
    echo "Starting $LONGNAME on $HOST"
    if [ $HOST == localhost  -o $HOST == "`hostname`" ] 
    then
       ${bin}/accumulo ${SERVICE} --address $1 >${ACCUMULO_LOG_DIR}/${SERVICE}_${LOGHOST}.out 2>${ACCUMULO_LOG_DIR}/${SERVICE}_${LOGHOST}.err & 
       MAX_FILES_OPEN=`bash -c 'ulimit -n'`
    else
       $SSH $HOST "bash -c 'exec nohup ${bin}/accumulo ${SERVICE} --address $1 >${ACCUMULO_LOG_DIR}/${SERVICE}_${LOGHOST}.out 2>${ACCUMULO_LOG_DIR}/${SERVICE}_${LOGHOST}.err' &"
       MAX_FILES_OPEN=`$SSH $HOST "bash -c 'ulimit -n'"` 
    fi

    if [ -n "$MAX_FILES_OPEN" ] && [ -n "$SLAVES" ] ; then
       if [ "$SLAVES" -gt 10 ] && [ "$MAX_FILES_OPEN" -lt 65536 ]; then
          echo "WARN : Max files open on $HOST is $MAX_FILES_OPEN, recommend 65536"
       fi
    fi
else
    echo "$HOST : $LONGNAME already running (${PID})"
fi
