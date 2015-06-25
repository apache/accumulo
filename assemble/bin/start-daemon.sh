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

usage="Usage: start-daemon.sh <host> <service>"

rotate_log () {
  logfile=$1;
  max_retained=$2;
  if [[ ! $max_retained =~ ^[0-9]+$ ]] || [[ $max_retained -lt 1 ]] ; then
    echo "ACCUMULO_NUM_OUT_FILES should be a positive number, but was '$max_retained'"
    exit 1
  fi

  if [ -f "$logfile" ]; then # rotate logs
    while [ $max_retained -gt 1 ]; do
      prev=`expr $max_retained - 1`
      [ -f "$logfile.$prev" ] && mv -f "$logfile.$prev" "$logfile.$max_retained"
      max_retained=$prev
    done
    mv -f "$logfile" "$logfile.$max_retained";
  fi
}

if [[ $# -ne 2 ]]; then
  echo $usage
  exit 2
fi

. "$bin"/config.sh
. "$bin"/config-server.sh

HOST="$1"
ADDRESS=$HOST
host "$1" >/dev/null 2>&1
if [[ $? != 0 ]]; then
   LOGHOST=$HOST
else
   LOGHOST=$(host "$HOST" | head -1 | cut -d' ' -f1)
fi
SERVICE=$2

SLAVES=$(wc -l < "${ACCUMULO_CONF_DIR}/slaves")

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

# Check the pid file to figure out if its already running.
PID_FILE="${ACCUMULO_PID_DIR}/accumulo-${ACCUMULO_IDENT_STRING}-${SERVICE}.pid"
if [ -f ${PID_FILE} ]; then
   PID=`cat ${PID_FILE}`
   if kill -0 $PID 2>/dev/null; then
      # Starting an already-started service shouldn't be an error per LSB
      echo "$HOST : $SERVICE already running (${PID})"
      exit 0
   fi
else
   echo "Starting $SERVICE on $HOST"
fi

COMMAND="${bin}/accumulo"
if [ "${ACCUMULO_WATCHER}" = "true" ]; then
   COMMAND="${bin}/accumulo_watcher.sh ${LOGHOST}"
fi

OUTFILE="${ACCUMULO_LOG_DIR}/${SERVICE}_${LOGHOST}.out"
ERRFILE="${ACCUMULO_LOG_DIR}/${SERVICE}_${LOGHOST}.err"

# Rotate the .out and .err files
rotate_log "$OUTFILE" ${ACCUMULO_NUM_OUT_FILES}
rotate_log "$ERRFILE" ${ACCUMULO_NUM_OUT_FILES}

# Fork the process, store the pid
nohup ${NUMA_CMD} "$COMMAND" "${SERVICE}" --address "${ADDRESS}" >"$OUTFILE" 2>"$ERRFILE" < /dev/null &
echo $! > ${PID_FILE}

# Check the max open files limit and selectively warn
MAX_FILES_OPEN=$(ulimit -n)

if [[ -n $MAX_FILES_OPEN && -n $SLAVES ]] ; then
   MAX_FILES_RECOMMENDED=${MAX_FILES_RECOMMENDED:-32768}
   if (( SLAVES > 10 )) && (( MAX_FILES_OPEN < MAX_FILES_RECOMMENDED ))
   then
      echo "WARN : Max open files on $HOST is $MAX_FILES_OPEN, recommend $MAX_FILES_RECOMMENDED" >&2
   fi
fi
