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

function print_usage {
  cat <<EOF
Usage: service.sh <command> (<argument> ...)

Commands:
  start <host> <service>            Starts <service> on local <host>
  stop <host> <service> <signal>    Stops <service> using <signal> on local <host>

EOF
  exit 1
}

function invalid_args {
  echo -e "Invalid arguments: $1\n"
  print_usage
  exit 1
}

rotate_log () {
  logfile=$1;
  max_retained=$2;
  if [[ ! "$max_retained" =~ ^[0-9]+$ ]] || [[ "$max_retained" -lt 1 ]] ; then
    echo "ACCUMULO_NUM_OUT_FILES should be a positive number, but was '$max_retained'"
    exit 1
  fi

  if [ -f "$logfile" ]; then # rotate logs
    while [[ "$max_retained" -gt 1 ]]; do
      prev=$(( max_retained - 1))
      [ -f "$logfile.$prev" ] && mv -f "$logfile.$prev" "$logfile.$max_retained"
      max_retained=$prev
    done
    mv -f "$logfile" "$logfile.$max_retained";
  fi
}

function start_service() {

  if [[ $# -ne 2 ]]; then
    invalid_args "start command expects these arguments: <host> <service>"
  fi

  host="$1"
  service="$2"

  address=$host
  loghost=$host

  # When the hostname provided is the alias/shortname, try to use the FQDN to make
  # sure we send the right address to the Accumulo process.
  if [[ "$host" = "$(hostname -s)" ]]; then
    host="$(hostname -f)"
    address="$host"
  fi

  if [[ ${service} == "monitor" && ${ACCUMULO_MONITOR_BIND_ALL} == "true" ]]; then
    address="0.0.0.0"
  fi

  COMMAND="${ACCUMULO_BIN_DIR}/accumulo"
  if [ "${ACCUMULO_WATCHER}" = "true" ]; then
    COMMAND="${ACCUMULO_LIBEXEC_DIR}/accumulo-watcher.sh ${loghost}"
  fi

  OUTFILE="${ACCUMULO_LOG_DIR}/${service}_${loghost}.out"
  ERRFILE="${ACCUMULO_LOG_DIR}/${service}_${loghost}.err"

  # Rotate the .out and .err files
  rotate_log "$OUTFILE" "${ACCUMULO_NUM_OUT_FILES}"
  rotate_log "$ERRFILE" "${ACCUMULO_NUM_OUT_FILES}"

  # NUMA sanity check
  if [[ $ACCUMULO_NUM_TSERVERS -eq 1 && -n $TSERVER_NUMA_OPTIONS ]]; then
    echo "TSERVER_NUMA_OPTIONS declared when ACCUMULO_NUM_TSERVERS is 1, use ACCUMULO_NUMACTL_OPTIONS instead"
    exit 1
  fi
  if [[ $ACCUMULO_NUM_TSERVERS -gt 1 && -n $TSERVER_NUMA_OPTIONS && ${#TSERVER_NUMA_OPTIONS[*]} -ne $ACCUMULO_NUM_TSERVERS ]]; then
    echo "TSERVER_NUMA_OPTIONS is declared, but not the same size as ACCUMULO_NUM_TSERVERS"
    exit 1
  fi

  if [[ "$service" != "tserver" || $ACCUMULO_NUM_TSERVERS -eq 1 ]]; then
    # Check the pid file to figure out if its already running.
    PID_FILE="${ACCUMULO_PID_DIR}/accumulo-${ACCUMULO_IDENT_STRING}-${service}.pid"
    if [[ -f "${PID_FILE}" ]]; then
      PID=$(cat "${PID_FILE}")
      if kill -0 "$PID" 2>/dev/null; then
        # Starting an already-started service shouldn't be an error per LSB
        echo "$host : $service already running (${PID})"
        exit 0
      fi
    fi
    echo "Starting $service on $host"

    ACCUMULO_ENABLE_NUMACTL=${ACCUMULO_ENABLE_NUMACTL:-"true"}
    ACCUMULO_NUMACTL_OPTIONS=${ACCUMULO_NUMACTL_OPTIONS:-"--interleave=all"}
    NUMA=$(which numactl 2>/dev/null)
    NUMACTL_EXISTS="$?"
    if [[ ( ${NUMACTL_EXISTS} -eq 0 ) && ( ${ACCUMULO_ENABLE_NUMACTL} == "true" ) ]] ; then
      export NUMA_CMD="${NUMA} ${ACCUMULO_NUMACTL_OPTIONS}"
    else
      export NUMA_CMD=""
    fi

    # Fork the process, store the pid
    nohup ${NUMA_CMD} "$COMMAND" "${service}" --address "${address}" >"$OUTFILE" 2>"$ERRFILE" < /dev/null &
    echo $! > "${PID_FILE}"

  else

    S="$service"
    for (( t=1; t<=ACCUMULO_NUM_TSERVERS; t++)); do

      service="$S-$t"

      # Check the pid file to figure out if its already running.
      PID_FILE="${ACCUMULO_PID_DIR}/accumulo-${ACCUMULO_IDENT_STRING}-${service}.pid"
      if [[ -f "${PID_FILE}" ]]; then
        PID=$(cat "${PID_FILE}")
        if kill -0 "$PID" 2>/dev/null; then
          # Starting an already-started service shouldn't be an error per LSB
          echo "$host : $service already running (${PID})"
          continue
        fi
      fi
      echo "Starting $service on $host"

      ACCUMULO_NUMACTL_OPTIONS=${ACCUMULO_NUMACTL_OPTIONS:-"--interleave=all"}
      ACCUMULO_NUMACTL_OPTIONS=${TSERVER_NUMA_OPTIONS[$t]}
      if [[ "$ACCUMULO_ENABLE_NUMACTL" == "true" ]]; then
        NUMA=$(which numactl 2>/dev/null)
        NUMACTL_EXISTS=$?
        if [[ ( ${NUMACTL_EXISTS} -eq 0 ) ]]; then
          export NUMA_CMD="${NUMA} ${ACCUMULO_NUMACTL_OPTIONS}"
        else
          export NUMA_CMD=""
        fi
      fi

      # We want the files to be consistently named with the log files
      # server_identifier_hostname.{out,err}, e.g. tserver_2_fqdn.out
      OUTFILE="${ACCUMULO_LOG_DIR}/${S}_${t}_${loghost}.out"
      ERRFILE="${ACCUMULO_LOG_DIR}/${S}_${t}_${loghost}.err"

      # Rotate the .out and .err files
      rotate_log "$OUTFILE" "${ACCUMULO_NUM_OUT_FILES}"
      rotate_log "$ERRFILE" "${ACCUMULO_NUM_OUT_FILES}"

      # Fork the process, store the pid
      nohup ${NUMA_CMD} "$COMMAND" "${service}" --address "${address}" >"$OUTFILE" 2>"$ERRFILE" < /dev/null &
      echo $! > "${PID_FILE}"

    done
  fi

  # Check the max open files limit and selectively warn
  MAX_FILES_OPEN=$(ulimit -n)

  if [[ -n $MAX_FILES_OPEN ]] ; then
    MAX_FILES_RECOMMENDED=${MAX_FILES_RECOMMENDED:-32768}
    if (( MAX_FILES_OPEN < MAX_FILES_RECOMMENDED ))
    then
      echo "WARN : Max open files on $host is $MAX_FILES_OPEN, recommend $MAX_FILES_RECOMMENDED" >&2
    fi
  fi
}

function stop_service() {

  if [[ $# -ne 3 ]]; then
    invalid_args "stop command expects these arguments: <host> <service> <signal>"
  fi

  host=$1
  service=$2
  signal=$3

  for pid_file in ${ACCUMULO_PID_DIR}/accumulo-${ACCUMULO_IDENT_STRING}-${service}*.pid; do
    if [[ -f "${pid_file}" ]]; then
      echo "Stopping $service on $host";
      kill -s "$signal" "$(cat "${pid_file}")" 2>/dev/null
      rm -f "${pid_file}" 2>/dev/null
    fi
  done
}

function main() {
  # Resolve libexec directory
  SOURCE="${BASH_SOURCE[0]}"
  while [[ -h "$SOURCE" ]]; do
    libexec="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
    SOURCE="$(readlink "$SOURCE")"
    [[ $SOURCE != /* ]] && SOURCE="$libexec/$SOURCE"
  done
  libexec="$( cd -P "$( dirname "$SOURCE" )" && pwd )"

  source "$libexec"/load-env.sh

  ACCUMULO_IDENT_STRING=${ACCUMULO_IDENT_STRING:-$USER}

  if [[ -z $1 ]]; then
    invalid_args "<command> cannot be empty"
  fi

  case "$1" in
    start)
      start_service "${@:2}"
      ;;
    stop)
      stop_service "${@:2}"
      ;;
    *)
      invalid_args "'$1' is an invalid <command>"
      ;;
  esac
}

main "$@"
