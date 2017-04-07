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

# Guarantees that Accumulo and its environment variables are set for start
# and stop scripts.  Should always be run after config.sh.
#
# Parameters checked by script
#  ACCUMULO_VERIFY_ONLY set to skip actions that would alter the local filesystem
#
# Values set by script that can be user provided.  If not provided script attempts to infer.
#  MONITOR            Machine to run monitor daemon on. Used by start-here.sh script
#
# Iff ACCUMULO_VERIFY_ONLY is not set, this script will
#   * Check for standalone mode (lack of masters and slaves files)
#     - Do appropriate set up
#   * Ensure the presense of local role files (masters, slaves, gc, tracers)
#
# Values always set by script.
#  SSH                Default ssh parameters used to start daemons
#

unset MASTER1
if [[ -f "$ACCUMULO_CONF_DIR/masters" ]]; then
  MASTER1=$(egrep -v '(^#|^\s*$)' "$ACCUMULO_CONF_DIR/masters" | head -1)
fi

if [[ -z "${MONITOR}" ]] ; then
  MONITOR=$MASTER1
  if [[ -f "$ACCUMULO_CONF_DIR/monitor" ]]; then
      MONITOR=$(egrep -v '(^#|^\s*$)' "$ACCUMULO_CONF_DIR/monitor" | head -1)
  fi
  if [[ -z "${MONITOR}" ]] ; then
    echo "Could not infer a Monitor role. You need to either define the MONITOR env variable, define \"${ACCUMULO_CONF_DIR}/monitor\", or make sure \"${ACCUMULO_CONF_DIR}/masters\" is non-empty."
    exit 1
  fi
fi
if [[ ! -f "$ACCUMULO_CONF_DIR/tracers" && -z "${ACCUMULO_VERIFY_ONLY}" ]]; then
  if [[ -z "${MASTER1}" ]] ; then
    echo "Could not find a master node to use as a default for the tracer role. Either set up \"${ACCUMULO_CONF_DIR}/tracers\" or make sure \"${ACCUMULO_CONF_DIR}/masters\" is non-empty."
    exit 1
  else
    echo "$MASTER1" > "$ACCUMULO_CONF_DIR/tracers"
  fi

fi

if [[ ! -f "$ACCUMULO_CONF_DIR/gc" && -z "${ACCUMULO_VERIFY_ONLY}" ]]; then
  if [[ -z "${MASTER1}" ]] ; then
    echo "Could not infer a GC role. You need to either set up \"${ACCUMULO_CONF_DIR}/gc\" or make sure \"${ACCUMULO_CONF_DIR}/masters\" is non-empty."
    exit 1
  else
    echo "$MASTER1" > "$ACCUMULO_CONF_DIR/gc"
  fi
fi

SSH='ssh -qnf -o ConnectTimeout=2'

# ACCUMULO-1985 provide a way to use the scripts and still bind to all network interfaces
export ACCUMULO_MONITOR_BIND_ALL=${ACCUMULO_MONITOR_BIND_ALL:-"false"}

if [[ -z "${ACCUMULO_PID_DIR}" ]]; then
  export ACCUMULO_PID_DIR="${ACCUMULO_HOME}/run"
fi
[[ -z ${ACCUMULO_VERIFY_ONLY} ]] && mkdir -p "${ACCUMULO_PID_DIR}" 2>/dev/null

if [[ -z "${ACCUMULO_IDENT_STRING}" ]]; then
  export ACCUMULO_IDENT_STRING="$USER"
fi

# The number of .out and .err files to retain
export ACCUMULO_NUM_OUT_FILES=${ACCUMULO_NUM_OUT_FILES:-5}
