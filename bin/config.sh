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

if [ -f $ACCUMULO_HOME/conf/accumulo-env.sh ] ; then
   . $ACCUMULO_HOME/conf/accumulo-env.sh
else
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

mkdir -p $ACCUMULO_LOG_DIR 2>/dev/null

export ACCUMULO_LOG_DIR

if [ -z "${ACCUMULO_VERSION}" ]; then
   ACCUMULO_VERSION=1.6.0-SNAPSHOT
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

MASTER1=$(egrep -v '(^#|^\s*$)' "$ACCUMULO_HOME/conf/masters" | head -1)
GC=$MASTER1
MONITOR=$MASTER1
if [ -f "$ACCUMULO_HOME/conf/gc" ]; then
   GC=$(egrep -v '(^#|^\s*$)' "$ACCUMULO_HOME/conf/gc" | head -1)
fi
if [ -f "$ACCUMULO_HOME/conf/monitor" ]; then
   MONITOR=$(egrep -v '(^#|^\s*$)' "$ACCUMULO_HOME/conf/monitor" | head -1)
fi
if [ ! -f "$ACCUMULO_HOME/conf/tracers" ]; then
   echo "$MASTER1" > "$ACCUMULO_HOME/conf/tracers"
fi

SSH='ssh -qnf -o ConnectTimeout=2'

export HADOOP_HOME=$HADOOP_PREFIX
export HADOOP_HOME_WARN_SUPPRESS=true

# See HADOOP-7154 and ACCUMULO-847
export MALLOC_ARENA_MAX=${MALLOC_ARENA_MAX:-1}
