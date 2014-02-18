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

# Guarantees that Accumulo and its environment variables are set.
#
# Parameters checked by script
#  ACCUMULO_VERIFY_ONLY set to skip actions that would alter the local filesystem
#
# Values set by script that can be user provided.  If not provided script attempts to infer.
#  ACCUMULO_CONF_DIR  Location where accumulo-env.sh, accumulo-site.xml and friends will be read from
#  ACCUMULO_HOME      Home directory for Accumulo
#  ACCUMULO_LOG_DIR   Directory for Accumulo daemon logs
#  ACCUMULO_VERSION   Accumulo version name
#  HADOOP_PREFIX      Prefix to the home dir for hadoop.
#  MONITOR            Machine to run monitor daemon on. Used by start-here.sh script
#
# Iff ACCUMULO_VERIFY_ONLY is not set, this script will
#   * Check for standalone mode (lack of masters and slaves files)
#     - Do appropriate set up
#   * Ensure the existence of ACCUMULO_LOG_DIR on the current host
#   * Ensure the presense of local role files (masters, slaves, gc, tracers)
#
# Values always set by script.
#  SSH                Default ssh parameters used to start daemons
#  MALLOC_ARENA_MAX   To work around a memory management bug (see ACCUMULO-847)
#  HADOOP_HOME        Home dir for hadoop.  TODO fix this.

if [ -z "${ACCUMULO_HOME}" ] ; then
  # Start: Resolve Script Directory
  SOURCE="${BASH_SOURCE[0]}"
  while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
     bin="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
     SOURCE="$(readlink "$SOURCE")"
     [[ $SOURCE != /* ]] && SOURCE="$bin/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
  done
  bin="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  script=$( basename "$SOURCE" )
  # Stop: Resolve Script Directory

  ACCUMULO_HOME=$( cd -P ${bin}/.. && pwd )
  export ACCUMULO_HOME
fi

if [ ! -d "${ACCUMULO_HOME}" ]; then
  echo "ACCUMULO_HOME=${ACCUMULO_HOME} is not a valid directory. Please make sure it exists"
  exit 1
fi

ACCUMULO_CONF_DIR="${ACCUMULO_CONF_DIR:-$ACCUMULO_HOME/conf}"
export ACCUMULO_CONF_DIR
if [ -z "$ACCUMULO_CONF_DIR" -o ! -d "$ACCUMULO_CONF_DIR" ]
then
  echo "ACCUMULO_CONF_DIR=$ACCUMULO_CONF_DIR is not a valid directory.  Please make sure it exists"
  exit 1
fi

if [ -f $ACCUMULO_CONF_DIR/accumulo-env.sh ] ; then
   . $ACCUMULO_CONF_DIR/accumulo-env.sh
elif [ -z "$ACCUMULO_TEST" ] ; then
   #
   # Attempt to bootstrap configuration and continue
   #
   echo ""
   echo "Accumulo is not properly configured."
   echo ""
   echo "Try running \$ACCUMULO_HOME/bin/bootstrap_config.sh and then editing"
   echo "\$ACCUMULO_HOME/conf/accumulo-env.sh"
   echo ""
   exit 1
fi

if [ -z ${ACCUMULO_LOG_DIR} ]; then
   ACCUMULO_LOG_DIR=$ACCUMULO_HOME/logs
fi

if [ -z "${ACCUMULO_VERIFY_ONLY}" ] ; then
  mkdir -p $ACCUMULO_LOG_DIR 2>/dev/null
fi

export ACCUMULO_LOG_DIR

if [ -z "$HADOOP_PREFIX" ]
then
   HADOOP_PREFIX="`which hadoop`"
   if [ -z "$HADOOP_PREFIX" ]
   then
      echo "You must set HADOOP_PREFIX"
      exit 1
   fi
   HADOOP_PREFIX=`dirname $HADOOP_PREFIX`
   HADOOP_PREFIX=`dirname $HADOOP_PREFIX`
fi
if [ ! -d "$HADOOP_PREFIX" ]
then
   echo "$HADOOP_PREFIX is not a directory"
   exit 1
fi
export HADOOP_PREFIX

MASTER1=$(egrep -v '(^#|^\s*$)' "$ACCUMULO_CONF_DIR/masters" | head -1)
if [ -z "${MONITOR}" ] ; then
  MONITOR=$MASTER1
  if [ -f "$ACCUMULO_CONF_DIR/monitor" ]; then
      MONITOR=$(egrep -v '(^#|^\s*$)' "$ACCUMULO_CONF_DIR/monitor" | head -1)
  fi
  if [ -z "${MONITOR}" ] ; then
    echo "Could not infer a Monitor role. You need to either define the MONITOR env variable, define \"${ACCUMULO_CONF_DIR}/monitor\", or make sure \"${ACCUMULO_CONF_DIR}/masters\" is non-empty."
    exit 1
  fi
fi
if [ ! -f "$ACCUMULO_CONF_DIR/tracers" -a -z "${ACCUMULO_VERIFY_ONLY}" ]; then
  if [ -z "${MASTER1}" ] ; then
    echo "Could not find a master node to use as a default for the tracer role. Either set up \"${ACCUMULO_CONF_DIR}/tracers\" or make sure \"${ACCUMULO_CONF_DIR}/masters\" is non-empty."
    exit 1
  else
    echo "$MASTER1" > "$ACCUMULO_CONF_DIR/tracers"
  fi

fi

if [ ! -f "$ACCUMULO_CONF_DIR/gc" -a -z "${ACCUMULO_VERIFY_ONLY}" ]; then
  if [ -z "${MASTER1}" ] ; then
    echo "Could not infer a GC role. You need to either set up \"${ACCUMULO_CONF_DIR}/gc\" or make sure \"${ACCUMULO_CONF_DIR}/masters\" is non-empty."
    exit 1
  else
    echo "$MASTER1" > "$ACCUMULO_CONF_DIR/gc"
  fi
fi

SSH='ssh -qnf -o ConnectTimeout=2'

export HADOOP_HOME=$HADOOP_PREFIX
export HADOOP_HOME_WARN_SUPPRESS=true

# See HADOOP-7154 and ACCUMULO-847
export MALLOC_ARENA_MAX=${MALLOC_ARENA_MAX:-1}

# ACCUMULO-1985 provide a way to use the scripts and still bind to all network interfaces
export ACCUMULO_MONITOR_BIND_ALL=${ACCUMULO_MONITOR_BIND_ALL:-"false"}
