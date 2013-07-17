#!/usr/bin/env bash

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


#copied below from hadoop-config.sh
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

ACCUMULO_HOME=`dirname "$this"`/../../../..
export ACCUMULO_HOME=`cd $ACCUMULO_HOME; pwd`

if [ -f $ACCUMULO_CONF_DIR/accumulo-env.sh ] ; then
. $ACCUMULO_CONF_DIR/accumulo-env.sh
fi

if [ -z "$HADOOP_PREFIX" ] ; then
    echo "HADOOP_PREFIX is not set.  Please make sure it's set globally."
    exit 1
fi

if [ "$1" = "" ] ; then
     echo "Usage: start-local.sh <startNode>"
     exit 1
fi

RW_HOME="$ACCUMULO_HOME/test/system/randomwalk"

cd "$RW_HOME"

# grab config from HDFS
"$HADOOP_PREFIX/bin/hadoop" fs -get /randomwalk/config.tgz config.tgz

# extract config to a tmp directory
TEMP="`hostname`-tmp"
rm -rf "$TEMP"
mkdir "$TEMP"
tar xzf config.tgz -C "$TEMP"
rm config.tgz

# config the logging
RW_LOGS="$RW_HOME/logs"
if [ ! -d "$RW_LOGS" ] 
then
   mkdir -p "$RW_LOGS" || ( echo "cannot create $RW_LOGS" ; exit 1 )
fi
LOG_ID=`hostname -s`_`date +%Y%m%d_%H%M%S`

# run the local walker
"$ACCUMULO_HOME/bin/accumulo" org.apache.accumulo.test.randomwalk.Framework --configDir "$RW_HOME/$TEMP/conf/" --logDir "$RW_LOGS" --logId "$LOG_ID" --module "$1" >"$RW_LOGS/$LOG_ID.out" 2>"$RW_LOGS/$LOG_ID.err" &
