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

# Really, we still support the third <long_name> argument, but let's not tell people that..
usage="Usage: start-server.sh <host> <service>"

# Support the 3-arg invocation for backwards-compat
if [[ $# -ne 2 ]] && [[ $# -ne 3 ]]; then
  echo $usage
  exit 2
fi

. "$bin"/config.sh
. "$bin"/config-server.sh

HOST="$1"
SERVICE="$2"

IFCONFIG=/sbin/ifconfig
[[ ! -x $IFCONFIG ]] && IFCONFIG='/bin/netstat -ie'

IP=$($IFCONFIG 2>/dev/null| grep "inet[^6]" | awk '{print $2}' | sed 's/addr://' | grep -v 0.0.0.0 | grep -v 127.0.0.1 | head -n 1)
if [[ $? != 0 ]] ; then
   IP=$(python -c 'import socket as s; print s.gethostbyname(s.getfqdn())')
fi

if [[ $HOST == "localhost" || $HOST == $(hostname -f) || $HOST == $(hostname -s) || $HOST == $IP ]]; then
   "$bin/start-daemon.sh" "$HOST" "$SERVICE"
else
   # Ensure that the provided configuration directory is sent with the command
   echo $($SSH $HOST "bash -c 'ACCUMULO_CONF_DIR=${ACCUMULO_CONF_DIR} $bin/start-daemon.sh \"$HOST\" \"$SERVICE\"'")
fi
