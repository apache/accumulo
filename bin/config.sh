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
# Values set by script that can be user provided.  If not provided script attempts to infer.
#  ACCUMULO_CONF_DIR  Location where accumulo-env.sh, accumulo-site.xml and friends will be read from
#  ACCUMULO_HOME      Home directory for Accumulo
#  ACCUMULO_LOG_DIR   Directory for Accumulo daemon logs
#  ACCUMULO_VERSION   Accumulo version name
#  HADOOP_HOME        Home dir for hadoop.
# 
# Values always set by script.
#  GC                 Machine to rn GC daemon on.  Used by start-here.sh script
#  MONITOR            Machine to run monitor daemon on. Used by start-here.sh script
#  SSH                Default ssh parameters used to start daemons

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

mkdir -p $ACCUMULO_LOG_DIR 2>/dev/null

export ACCUMULO_LOG_DIR

if [ -z ${ACCUMULO_VERSION} ]; then
        ACCUMULO_VERSION=1.4.4
fi

if [ -z "$HADOOP_HOME" ]
then
   HADOOP_HOME="`which hadoop`"
   if [ -z "$HADOOP_HOME" ]
   then
      echo "You must set HADOOP_HOME"
      exit 1
   fi
   HADOOP_HOME=`dirname $HADOOP_HOME`
   HADOOP_HOME=`dirname $HADOOP_HOME`
fi
if [ ! -d "$HADOOP_HOME" ]
then
    echo "$HADOOP_HOME is not a directory"
    exit 1
fi
export HADOOP_HOME

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
MASTER1=`grep -v '^#' "$ACCUMULO_CONF_DIR/masters" | head -1`
GC=$MASTER1
MONITOR=$MASTER1
if [ -f "$ACCUMULO_CONF_DIR/gc" ]; then
    GC=`grep -v '^#' "$ACCUMULO_CONF_DIR/gc" | head -1`
fi
if [ -f "$ACCUMULO_CONF_DIR/monitor" ]; then
    MONITOR=`grep -v '^#' "$ACCUMULO_CONF_DIR/monitor" | head -1`
fi
if [ ! -f "$ACCUMULO_CONF_DIR/tracers" ]; then
    echo "$MASTER1" > "$ACCUMULO_CONF_DIR/tracers"
fi
SSH='ssh -qnf -o ConnectTimeout=2'

# See HADOOP-7154 and ACCUMULO-847
export MALLOC_ARENA_MAX=${MALLOC_ARENA_MAX:-1}
