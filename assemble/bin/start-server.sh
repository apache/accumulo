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
while [[ -h "$SOURCE" ]]; do # resolve $SOURCE until the file is no longer a symlink
   bin="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
   SOURCE="$(readlink "$SOURCE")"
   [[ $SOURCE != /* ]] && SOURCE="$bin/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
bin="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
script=$( basename "$SOURCE" )
# Stop: Resolve Script Directory

. "$bin"/config.sh
. "$bin"/config-server.sh

HOST="$1"
host "$1" >/dev/null 2>/dev/null
if [[ $? != 0 ]]; then
   LOGHOST="$1"
else
   LOGHOST=$(host "$1" | head -1 | cut -d' ' -f1)
fi
ADDRESS=$1
SERVICE=$2
LONGNAME=$3
[[ -z $LONGNAME ]] && LONGNAME=$2

SLAVES=$(wc -l < "${ACCUMULO_CONF_DIR}/slaves")

IFCONFIG=/sbin/ifconfig
[[ ! -x $IFCONFIG ]] && IFCONFIG='/bin/netstat -ie'


IP=$($IFCONFIG 2>/dev/null| grep "inet[^6]" | awk '{print $2}' | sed 's/addr://' | grep -v 0.0.0.0 | grep -v 127.0.0.1 | head -n 1)
if [[ $? != 0 ]] ; then
   IP=$(python -c 'import socket as s; print s.gethostbyname(s.getfqdn())')
fi

# When the hostname provided is the alias/shortname, try to use the FQDN to make
# sure we send the right address to the Accumulo process.
if [[ "$HOST" = "$(hostname -s)" ]]; then
    HOST="$(hostname -f)"
    ADDRESS="$HOST"
fi

# ACCUMULO-1985 Allow monitor to bind on all interfaces
if [[ ${SERVICE} == "monitor" && ${ACCUMULO_MONITOR_BIND_ALL} == "true" ]]; then
    ADDRESS="0.0.0.0"
fi

if [[ $HOST == localhost || $HOST == "$(hostname -f)" || $HOST = "$IP" ]]; then
   PID=$(ps -ef | egrep ${ACCUMULO_HOME}/.*/accumulo.*.jar | grep "Main $SERVICE" | grep -v grep | awk {'print $2'} | head -1)
else
   PID=$($SSH "$HOST" ps -ef | egrep "${ACCUMULO_HOME}/.*/accumulo.*.jar" | grep "Main $SERVICE" | grep -v grep | awk {'print $2'} | head -1)
fi

if [[ -z "$PID" ]]; then
   echo "Starting $LONGNAME on $HOST"
   COMMAND="${bin}/accumulo"
   if [ "${ACCUMULO_WATCHER}" = "true" ]; then
      COMMAND="${bin}/accumulo_watcher.sh ${LOGHOST}"
   fi

   if [ "$HOST" = "localhost" -o "$HOST" = "`hostname -f`" -o "$HOST" = "$ip" ]; then
      ${bin}/accumulo ${SERVICE} --address ${ADDRESS} >${ACCUMULO_LOG_DIR}/${SERVICE}_${LOGHOST}.out 2>${ACCUMULO_LOG_DIR}/${SERVICE}_${LOGHOST}.err & 
      MAX_FILES_OPEN=$(ulimit -n)
   else
      $SSH $HOST "bash -c 'exec nohup ${bin}/accumulo ${SERVICE} --address ${ADDRESS} >${ACCUMULO_LOG_DIR}/${SERVICE}_${LOGHOST}.out 2>${ACCUMULO_LOG_DIR}/${SERVICE}_${LOGHOST}.err' &"
      MAX_FILES_OPEN=$($SSH $HOST "/usr/bin/env bash -c 'ulimit -n'") 
   fi

   if [[ -n $MAX_FILES_OPEN && -n $SLAVES ]] ; then
      MAX_FILES_RECOMMENDED=${MAX_FILES_RECOMMENDED:-32768}
      if (( SLAVES > 10 )) && (( MAX_FILES_OPEN < MAX_FILES_RECOMMENDED ))
      then
         echo "WARN : Max open files on $HOST is $MAX_FILES_OPEN, recommend $MAX_FILES_RECOMMENDED" >&2
      fi
   fi
else
   echo "$HOST : $LONGNAME already running (${PID})"
fi
