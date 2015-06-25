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
   bin=$( cd -P "$( dirname "$SOURCE" )" && pwd )
   SOURCE=$(readlink "$SOURCE")
   [[ $SOURCE != /* ]] && SOURCE="$bin/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
bin=$( cd -P "$( dirname "$SOURCE" )" && pwd )
# Stop: Resolve Script Directory

. "$bin"/config.sh
. "$bin"/config-server.sh

HOST=$1

IFCONFIG=/sbin/ifconfig
[[ ! -x $IFCONFIG ]] && IFCONFIG='/bin/netstat -ie'

IP=$($IFCONFIG 2>/dev/null| grep "inet[^6]" | awk '{print $2}' | sed 's/addr://' | grep -v 0.0.0.0 | grep -v 127.0.0.1 | head -n 1)
if [[ $? != 0 ]]
then
   IP=$(python -c 'import socket as s; print s.gethostbyname(s.getfqdn())')
fi

# only stop if there's not one already running
PID_FILE="${ACCUMULO_PID_DIR}/accumulo-${ACCUMULO_IDENT_STRING}-${3}.pid"
if [[ $HOST == localhost || $HOST = "$(hostname -s)" || $HOST = "$(hostname -f)" || $HOST = "$IP" ]] ; then
   if [ -f ${PID_FILE} ]; then
      echo "Stopping $3 on $1";
      kill -s "$4" `cat ${PID_FILE}` 2>/dev/null
      rm -f ${PID_FILE} 2>/dev/null
   fi;
else
   PID=$(ssh -q -o 'ConnectTimeout 8' "$1" cat "${PID_FILE}" 2>/dev/null)
   if [[ ! -z $PID ]]; then
      echo "Stopping $3 on $1";
      ssh -q -o 'ConnectTimeout 8' "$1" "kill -s $4 $PID 2>/dev/null; rm -f ${PID_FILE} 2>/dev/null"
   fi
fi
