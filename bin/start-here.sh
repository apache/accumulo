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

IFCONFIG=/sbin/ifconfig
if [ ! -x $IFCONFIG ]; then
   IFCONFIG='/bin/netstat -ie'
fi
ip=$($IFCONFIG 2>/dev/null| grep inet[^6] | awk '{print $2}' | sed 's/addr://' | grep -v 0.0.0.0 | grep -v 127.0.0.1 | head -n 1)
if [ $? != 0 ]; then
   ip=$(python -c 'import socket as s; print s.gethostbyname(s.getfqdn())')
fi

HOSTS="`hostname -a` `hostname` localhost 127.0.0.1 $ip"
for host in $HOSTS; do
   if grep -q "^${host}\$" $ACCUMULO_CONF_DIR/slaves; then
      ${bin}/start-server.sh $host tserver "tablet server"
      break
   fi
done

for host in $HOSTS; do
   if grep -q "^${host}\$" $ACCUMULO_CONF_DIR/masters; then
      ${bin}/accumulo org.apache.accumulo.server.master.state.SetGoalState NORMAL
      ${bin}/start-server.sh $host master
      break
   fi
done

for host in $HOSTS; do
   if [ ${host} = "${GC}" ]; then
      ${bin}/start-server.sh $GC gc "garbage collector"
      break
   fi
done

for host in $HOSTS; do
   if [ ${host} = "${MONITOR}" ]; then
      ${bin}/start-server.sh $MONITOR monitor 
      break
   fi
done

for host in $HOSTS; do
   if grep -q "^${host}\$" $ACCUMULO_CONF_DIR/tracers; then
      ${bin}/start-server.sh $host tracer 
      break
   fi
done
