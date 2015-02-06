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

LOGHOST=$1
shift
process=$1

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
   bin="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
   SOURCE="$(readlink "$SOURCE")"
   [[ $SOURCE != /* ]] && SOURCE="$bin/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
bin="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
# Stop: Resolve Script Directory

. "${bin}"/config.sh

ERRFILE=${ACCUMULO_LOG_DIR}/${process}_${LOGHOST}.err
OUTFILE=${ACCUMULO_LOG_DIR}/${process}_${LOGHOST}.out
DEBUGLOG=${ACCUMULO_LOG_DIR}/${process}_$(hostname).debug.log
export COMMAND="${bin}/accumulo \"\$@\""

logger -s "starting process $process at $(date)"
stopRunning=""
while [ -z "$stopRunning" ];
do
  eval $COMMAND 2> $ERRFILE
  exit=$?
  unset cause
  if [ "$exit" -eq 0 ]; then
    potentialStopRunning="Clean Exit"
  elif [ "$exit" -eq 1 ]; then
    potentialStopRunning="Unexpected error"
  elif [ "$exit" -eq 130 ]; then
    stopRunning="Control C detected, exiting"
  elif [ "$exit" -eq 143 ]; then
    stopRunning="Process terminated, exiting"
  elif [ "$exit" -eq 137 ]; then
    potentialStopRunning="Process killed, exiting"
  fi
  if [ -z "$stopRunning" ]; then
    stopRunning=$potentialStopRunning;

    if [ $exit -eq 1 ]; then
      source="exit code"
      cause="Unexpected Exception"
    elif tail -n50 $OUTFILE | grep "java.lang.OutOfMemoryError:" > /dev/null; then
      source="logs"
      cause="Out of memory exception"
    elif [ "$process" = "tserver" ]; then
      if tail -n50 $DEBUGLOG | grep "ERROR: Lost tablet server lock (reason =" > /dev/null ; then
        source="logs"
        cause="ZKLock lost"
      fi
    elif [ "$process" = "master" ]; then
      if tail -n50 $DEBUGLOG | grep "ERROR: Master lock in zookeeper lost (reason =" > /dev/null ; then
        source="logs"
        cause="ZKLock lost"
      fi
    elif [ "$process" = "gc" ]; then
      if tail -n50 $DEBUGLOG | grep "FATAL: GC lock in zookeeper lost (reason =" > /dev/null ; then
        source="logs"
        cause="ZKLock lost"
      fi
    elif [ "$process" = "monitor" ]; then
      if tail -n50 $DEBUGLOG | grep "ERROR:  Monitor lock in zookeeper lost (reason =" > /dev/null ; then
        source="logs"
        cause="ZKLock lost"
      fi
    elif [ $exit -ne 0 ]; then
      source="exit code"
      cause="Unknown error"
    fi
    case $cause in
      #Unknown exit code
      "Unknown error")
        #window doesn't matter when retries = 0
        RETRIES=0
        ;;

      "Unexpected Exception")
        WINDOW=$UNEXPECTED_TIMESPAN
        RETRIES=$UNEXPECTED_RETRIES
        ;;

      "Out of memory exception") 
        WINDOW=$OOM_TIMESPAN
        RETRIES=$OOM_RETRIES
        ;;

      "ZKLock lost")
        WINDOW=$ZKLOCK_TIMESPAN
        RETRIES=$ZKLOCK_RETRIES
        ;;
    esac

    if [ -n "$cause" ]; then
      stopRunning=""
      declare -i attempts
      attempts="`jobs | grep "reason$cause" | wc -l`+1"
      if [ "$RETRIES" -le $attempts ]; then
        stopRunning="$process encountered $cause in $source with exit code $exit- quitting ($attempts/$RETRIES in $WINDOW seconds)"
        # kill all sleeps now
        for list in `jobs | cut -b 2-2`; do kill %$list; done
      else
        logger -s "$process encountered $cause in $source with exit code $exit- retrying ($attempts/$RETRIES in $WINDOW seconds)"
        eval "(sleep $WINDOW ; echo "reason$cause" >> /dev/null) &" 
      fi
    fi 
  fi
done
logger -s $stopRunning
