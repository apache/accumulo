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
Usage: cluster.sh <command> (<argument> ...)

Commands:
  start-all [--notTservers]       Starts all services on cluster
  start-tservers                  Starts all tservers on cluster
  start-here                      Starts all services on this node
  start-service <host> <service>  Starts <service> on <host>
  stop-all                        Stops all services on cluster
  stop-tservers                   Stops all tservers on cluster
  stop-here                       Stops all services on this node

EOF
  exit 1
}

function invalid_args {
  echo -e "Invalid arguments: $1\n"
  print_usage 1>&2
  exit 1
}

function get_ip() {
  ip_addr=$(ip addr | grep 'state UP' -A2 | tail -n1 | awk '{print $2}' | cut -f1  -d'/')
  if [[ $? != 0 ]]; then
    ip_addr=$(getent ahosts "$(hostname -f)" | grep DGRAM | cut -f 1 -d ' ')
  fi
  echo "$ip_addr"
}

function start_service() {
  host="$1"
  service="$2"

  if [[ $host == "localhost" || $host == $(hostname -f) || $host == $(hostname -s) || $host == $(get_ip) ]]; then
    "$libexec/service.sh" start "$host" "$service"
  else
    $SSH "$host" "bash -c 'ACCUMULO_CONF_DIR=${ACCUMULO_CONF_DIR} $libexec/service.sh start \"$host\" \"$service\"'"
  fi
}

function start_tservers() {
  echo -n "Starting tablet servers ..."
  count=1
  for server in $(egrep -v '(^#|^\s*$)' "${ACCUMULO_CONF_DIR}/tservers"); do
    echo -n "."
    start_service "$server" tserver &
    if (( ++count % 72 == 0 )) ;
    then
      echo
      wait
    fi
  done
  echo " done"
}

function start_all() {
  unset DISPLAY

  start_service "$monitor" monitor 

  if [ "$1" != "--notTservers" ]; then
    start_tservers
  fi

  for master in $(egrep -v '(^#|^\s*$)' "$ACCUMULO_CONF_DIR/masters"); do
    start_service "$master" master
  done

  for gc in $(egrep -v '(^#|^\s*$)' "$ACCUMULO_CONF_DIR/gc"); do
    start_service "$gc" gc
  done

  for tracer in $(egrep -v '(^#|^\s*$)' "$ACCUMULO_CONF_DIR/tracers"); do
    start_service "$tracer" tracer
  done
}

function start_here() {

  local_hosts="$(hostname -a 2> /dev/null) $(hostname) localhost 127.0.0.1 $(get_ip)"
  for host in $local_hosts; do
    if grep -q "^${host}\$" "$ACCUMULO_CONF_DIR/tservers"; then
      start_service "$host" tserver
      break
    fi
  done

  for host in $local_hosts; do
    if grep -q "^${host}\$" "$ACCUMULO_CONF_DIR/masters"; then
      start_service "$host" master
      break
    fi
  done

  for host in $local_hosts; do
    if grep -q "^${host}\$" "$ACCUMULO_CONF_DIR/gc"; then
      start_service "$host" gc
      break
    fi
  done

  for host in $local_hosts; do
    if [[ $host == "$monitor" ]]; then
      start_service "$monitor" monitor 
      break
    fi
  done

  for host in $local_hosts; do
    if grep -q "^${host}\$" "$ACCUMULO_CONF_DIR/tracers"; then
      start_service "$host" tracer 
      break
    fi
  done
}

function stop_service() {
  host="$1"
  service="$2"
  signal="$3"

  # only stop if there's not one already running
  if [[ $host == localhost || $host = "$(hostname -s)" || $host = "$(hostname -f)" || $host = $(get_ip) ]] ; then
    "$libexec/service.sh" stop "$host" "$service" "$signal"
  else
    $SSH "$host" "bash -c '$libexec/service.sh stop \"$host\" \"$service\" \"$signal\"'"
  fi
}

function stop_tservers() {
  tserver_hosts=$(egrep -v '(^#|^\s*$)' "${ACCUMULO_CONF_DIR}/tservers")

  echo "Stopping unresponsive tablet servers (if any)..."
  for server in ${tserver_hosts}; do
    # only start if there's not one already running
    stop_service "$server" tserver TERM & 
  done

  sleep 10

  echo "Stopping unresponsive tablet servers hard (if any)..."
  for server in ${tserver_hosts}; do
    # only start if there's not one already running
    stop_service "$server" tserver KILL & 
  done

  echo "Cleaning tablet server entries from zookeeper"
  ${accumulo_cmd} org.apache.accumulo.server.util.ZooZap -tservers
}

function stop_all() {
  echo "Stopping accumulo services..."
  if ! ${accumulo_cmd} admin stopAll
  then
    echo "Invalid password or unable to connect to the master"
    echo "Initiating forced shutdown in 15 seconds (Ctrl-C to abort)"
    sleep 10
    echo "Initiating forced shutdown in  5 seconds (Ctrl-C to abort)"
  else
    echo "Accumulo shut down cleanly"
    echo "Utilities and unresponsive servers will shut down in 5 seconds (Ctrl-C to abort)"
  fi

  sleep 5

  #look for master and gc processes not killed by 'admin stopAll'
  for signal in TERM KILL ; do
    for master in $(grep -v '^#' "$ACCUMULO_CONF_DIR/masters"); do
      stop_service "$master" master $signal
    done

    for gc in $(grep -v '^#' "$ACCUMULO_CONF_DIR/gc"); do
      stop_service "$gc" gc $signal
    done

    stop_service "$monitor" monitor $signal

    for tracer in $(egrep -v '(^#|^\s*$)' "$ACCUMULO_CONF_DIR/tracers"); do
      stop_service "$tracer" tracer $signal
    done
  done

  # stop tserver still running
  stop_tservers

  echo "Cleaning all server entries in ZooKeeper"
  ${accumulo_cmd} org.apache.accumulo.server.util.ZooZap -master -tservers -tracers --site-file "$ACCUMULO_CONF_DIR/accumulo-site.xml"
}

function stop_here() {
  # Determine hostname without errors to user
  hosts_to_check=($(hostname -a 2> /dev/null | head -1) $(hostname -f))

  if egrep -q localhost\|127.0.0.1 "$ACCUMULO_CONF_DIR/tservers"; then
    ${accumulo_cmd} admin stop localhost
  else
    for host in "${hosts_to_check[@]}"; do
      if grep -q "$host" "$ACCUMULO_CONF_DIR"/tservers; then
        ${accumulo_cmd} admin stop "$host"
      fi
    done
  fi

  for host in "${hosts_to_check[@]}"; do
    for signal in TERM KILL; do
      for svc in tserver gc master monitor tracer; do
        stop_service "$host" $svc $signal
      done
    done
  done
}

function main() {
  # Start: Resolve Script Directory
  SOURCE="${BASH_SOURCE[0]}"
  while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
    libexec="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
    SOURCE="$(readlink "$SOURCE")"
    [[ $SOURCE != /* ]] && SOURCE="$libexec/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
  done
  libexec="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  # Stop: Resolve Script Directory

  source "$libexec"/load-env.sh

  if [[ -f $ACCUMULO_CONF_DIR/slaves ]]; then
    echo "ERROR: A 'slaves' file was found in $ACCUMULO_CONF_DIR/"
    echo "Accumulo now reads tablet server hosts from 'tservers' and requires that the 'slaves' file not be present to reduce confusion."
    echo "Please rename the 'slaves' file to 'tservers' or remove it if both exist."
    exit 1
  fi

  if [[ ! -f $ACCUMULO_CONF_DIR/tservers ]]; then
    echo "ERROR: A 'tservers' file was not found at $ACCUMULO_CONF_DIR/tservers"
    echo "Please make sure it exists and is configured with tablet server hosts."
    exit 1
  fi

  unset master1
  if [[ -f "$ACCUMULO_CONF_DIR/masters" ]]; then
    master1=$(egrep -v '(^#|^\s*$)' "$ACCUMULO_CONF_DIR/masters" | head -1)
  fi

  if [[ -z "${monitor}" ]] ; then
    monitor=$master1
    if [[ -f "$ACCUMULO_CONF_DIR/monitor" ]]; then
      monitor=$(egrep -v '(^#|^\s*$)' "$ACCUMULO_CONF_DIR/monitor" | head -1)
    fi
    if [[ -z "${monitor}" ]] ; then
      echo "Could not infer a Monitor role. You need to either define \"${ACCUMULO_CONF_DIR}/monitor\"," 
      echo "or make sure \"${ACCUMULO_CONF_DIR}/masters\" is non-empty."
      exit 1
    fi
  fi
  if [[ ! -f "$ACCUMULO_CONF_DIR/tracers" ]]; then
    if [[ -z "${master1}" ]] ; then
      echo "Could not find a master node to use as a default for the tracer role."
      echo "Either set up \"${ACCUMULO_CONF_DIR}/tracers\" or make sure \"${ACCUMULO_CONF_DIR}/masters\" is non-empty."
      exit 1
    else
      echo "$master1" > "$ACCUMULO_CONF_DIR/tracers"
    fi
  fi

  if [[ ! -f "$ACCUMULO_CONF_DIR/gc" ]]; then
    if [[ -z "${master1}" ]] ; then
      echo "Could not infer a GC role. You need to either set up \"${ACCUMULO_CONF_DIR}/gc\" or make sure \"${ACCUMULO_CONF_DIR}/masters\" is non-empty."
      exit 1
    else
      echo "$master1" > "$ACCUMULO_CONF_DIR/gc"
    fi
  fi
  accumulo_cmd="$ACCUMULO_BIN_DIR/accumulo"

  SSH='ssh -qnf -o ConnectTimeout=2'

  if [[ -z $1 ]]; then
    invalid_args "<command> cannot be empty"
  fi

  case "$1" in
    start-all)
      start_all "${*:2}"
      ;;
    start-tservers)
      start_tservers
      ;;
    start-here)
      start_here
      ;;
    stop-all)
      stop_all
      ;;
    stop-tservers)
      stop_tservers
      ;;
    stop-here)
      stop_here
      ;;
    *)
      invalid_args "'$1' is an invalid <command>"
      ;;
  esac
}

main "$@"
