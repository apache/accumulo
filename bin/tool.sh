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
# Stop: Resolve Script Directory

. "$bin"/config.sh

if [ -z "$HADOOP_PREFIX" ] ; then
   echo "HADOOP_PREFIX is not set.  Please make sure it's set globally or in conf/accumulo-env.sh"
   exit 1
fi
if [ -z "$ZOOKEEPER_HOME" ] ; then
   echo "ZOOKEEPER_HOME is not set.  Please make sure it's set globally or in conf/accumulo-env.sh"
   exit 1
fi

ZOOKEEPER_CMD='ls -1 $ZOOKEEPER_HOME/zookeeper-[0-9]*[^csn].jar '
if [ `eval $ZOOKEEPER_CMD | wc -l` != "1" ] ; then
   echo "Not exactly one zookeeper jar in $ZOOKEEPER_HOME"
   exit 1
fi
ZOOKEEPER_LIB=$(eval $ZOOKEEPER_CMD)

LIB="$ACCUMULO_HOME/lib"
CORE_LIB="$LIB/accumulo-core.jar"
FATE_LIB="$LIB/accumulo-fate.jar"
THRIFT_LIB="$LIB/libthrift.jar"
TRACE_LIB="$LIB/accumulo-trace.jar"
JCOMMANDER_LIB="$LIB/jcommander.jar"
COMMONS_VFS_LIB="$LIB/commons-vfs2.jar"

USERJARS=" "
for arg in "$@"; do
    if [ "$arg" != "-libjars" -a -z "$TOOLJAR" ]; then
      TOOLJAR="$arg"
      shift
   elif [ "$arg" != "-libjars" -a -z "$CLASSNAME" ]; then
      CLASSNAME="$arg"
      shift
   elif [ -z "$USERJARS" ]; then
      USERJARS=$(echo "$arg" | tr "," " ")
      shift
   elif [ "$arg" = "-libjars" ]; then
      USERJARS=""
      shift
   else
      break
   fi
done

LIB_JARS="$THRIFT_LIB,$CORE_LIB,$FATE_LIB,$ZOOKEEPER_LIB,$TRACE_LIB,$JCOMMANDER_LIB,$COMMONS_VFS_LIB"
H_JARS="$THRIFT_LIB:$CORE_LIB:$FATE_LIB:$ZOOKEEPER_LIB:$TRACE_LIB:$JCOMMANDER_LIB:$COMMONS_VFS_LIB"

for jar in $USERJARS; do
   LIB_JARS="$LIB_JARS,$jar"
   H_JARS="$H_JARS:$jar"
done
export HADOOP_CLASSPATH="$H_JARS:$HADOOP_CLASSPATH"

if [ -z "$CLASSNAME" -o -z "$TOOLJAR" ]; then
   echo "Usage: tool.sh path/to/myTool.jar my.tool.class.Name [-libjars my1.jar,my2.jar]" 1>&2
   exit 1
fi

#echo USERJARS=$USERJARS
#echo CLASSNAME=$CLASSNAME
#echo HADOOP_CLASSPATH=$HADOOP_CLASSPATH
#echo exec "$HADOOP_PREFIX/bin/hadoop" jar "$TOOLJAR" $CLASSNAME -libjars \"$LIB_JARS\" $ARGS
exec "$HADOOP_PREFIX/bin/hadoop" jar "$TOOLJAR" $CLASSNAME -libjars \"$LIB_JARS\" "$@"
