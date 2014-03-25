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
#  HADOOP_HOME        Home dir for hadoop.
#  MONITOR            Machine to run monitor daemon on. Used by start-here.sh script
#
# If and only if ACCUMULO_VERIFY_ONLY is not set, this script will
#   * Check for standalone mode (lack of masters and slaves files)
#     - Do appropriate set up
#   * Ensure the existence of ACCUMULO_LOG_DIR on the current host
#   * Ensure the presense of local role files (masters, slaves, gc, tracers)
#
# Values always set by script.
#  SSH                Default ssh parameters used to start daemons
#  MALLOC_ARENA_MAX   To work around a memory management bug (see ACCUMULO-847)

if [ -z "${ACCUMULO_HOME}" ] ; then
  this="$0"
  while [ -h "$this" ]; do
      ls=`ls -ld "$this"`
      link=`expr "$ls" : '.*-> \(.*\)$'`
      if expr "$link" : '.*/.*' > /dev/null; then
          this="$link"
      else
          this=`dirname "$this"`/"$link"
      fi
  done
  bin=`dirname "$this"`
  script=`basename "$this"`
  bin=`cd "$bin"; pwd`
  this="$bin/$script"

  ACCUMULO_HOME=`dirname "$this"`/..
  export ACCUMULO_HOME=`cd $ACCUMULO_HOME; pwd`
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
fi

if [ -z ${ACCUMULO_LOG_DIR} ]; then
        ACCUMULO_LOG_DIR=$ACCUMULO_HOME/logs
fi

if [ -z "${ACCUMULO_VERIFY_ONLY}" ] ; then
  mkdir -p $ACCUMULO_LOG_DIR 2>/dev/null
fi

export ACCUMULO_LOG_DIR

if [ -z ${ACCUMULO_VERSION} ]; then
        ACCUMULO_VERSION=1.4.6-SNAPSHOT
fi

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

if [ -z "${ACCUMULO_VERIFY_ONLY}" ] ; then
  if [ ! -f "$ACCUMULO_CONF_DIR/masters" -o ! -f "$ACCUMULO_CONF_DIR/slaves" ]
  then
      if [ ! -f "$ACCUMULO_CONF_DIR/masters" -a ! -f "$ACCUMULO_CONF_DIR/slaves" ]
      then
          echo "STANDALONE: Missing both conf/masters and conf/slaves files"
          echo "STANDALONE: Assuming single-node (localhost only) instance"
          echo "STANDALONE: echo "`hostname`" > $ACCUMULO_CONF_DIR/masters"
          echo `hostname` > "$ACCUMULO_CONF_DIR/masters"
          echo "STANDALONE: echo "`hostname`" > $ACCUMULO_CONF_DIR/slaves"
          echo `hostname` > "$ACCUMULO_CONF_DIR/slaves"
          echo "STANDALONE: echo "`hostname`" > $ACCUMULO_CONF_DIR/monitor"
          echo `hostname` > "$ACCUMULO_CONF_DIR/monitor"
          fgrep -s logger.dir.walog "$ACCUMULO_CONF_DIR/accumulo-site.xml" > /dev/null
          WALOG_CONFIGURED=$?
          if [ $WALOG_CONFIGURED -ne 0 -a ! -e "$ACCUMULO_HOME/walogs" ]
          then
            echo "STANDALONE: Creating default local write-ahead log directory"
            mkdir "$ACCUMULO_HOME/walogs"
            echo "STANDALONE: mkdir \"$ACCUMULO_HOME/walogs\""
          fi
          if [ ! -e "$ACCUMULO_CONF_DIR/accumulo-metrics.xml" ]
          then
            echo "STANDALONE: Creating default metrics configuration"
            cp "$ACCUMULO_CONF_DIR/accumulo-metrics.xml.example" "$ACCUMULO_CONF_DIR/accumulo-metrics.xml"
          fi
      else
          echo "You are missing either $ACCUMULO_CONF_DIR/masters or $ACCUMULO_CONF_DIR/slaves"
          echo "Please configure them both for a multi-node instance, or delete them both for a single-node (localhost only) instance"
          exit 1
      fi
  fi
fi
MASTER1=`grep -v '^#' "$ACCUMULO_CONF_DIR/masters" | head -1`
if [ -z "${MONITOR}" ] ; then
  MONITOR=$MASTER1
  if [ -f "$ACCUMULO_CONF_DIR/monitor" ]; then
      MONITOR=`grep -v '^#' "$ACCUMULO_CONF_DIR/monitor" | head -1`
  fi
  if [ -z "${MONITOR}" ] ; then
    echo "Could not infer a Monitor role. You need to either define the MONITOR env variable, define \"${ACCUMULO_CONF_DIR}/monitor\", or make sure \"${ACCUMULO_CONF_DIR}/masters\" is non-empty."
    exit 1
  fi
fi

SSH='ssh -qnf -o ConnectTimeout=2'

# See HADOOP-7154 and ACCUMULO-847
export MALLOC_ARENA_MAX=${MALLOC_ARENA_MAX:-1}
