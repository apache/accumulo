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


bin=`dirname "$0"`
bin=`cd "$bin"; pwd`
. "$bin"/config.sh

if [ -z $HADOOP_HOME ] ; then
    echo "HADOOP_HOME is not set.  Please make sure it's set globally or in conf/accumulo-env.sh"
    exit 1
fi
if [ -z $ZOOKEEPER_HOME ] ; then
    echo "ZOOKEEPER_HOME is not set.  Please make sure it's set globally or in conf/accumulo-env.sh"
    exit 1
fi

LIB=$ACCUMULO_HOME/lib

ZOOKEEPER_CMD='ls -1 $ZOOKEEPER_HOME/zookeeper-[0-9]*[^csn].jar '
CORE_CMD='ls -1 $LIB/accumulo-core-*[^cs].jar'
THRIFT_CMD='ls -1 $LIB/libthrift-*[^cs].jar'
CLOUDTRACE_CMD='ls -1 $LIB/cloudtrace-*[^cs].jar'

if [ `eval $ZOOKEEPER_CMD | wc -l` != "1" ] ; then
    echo "Not exactly one zookeeper jar in $ZOOKEEPER_HOME"
    exit 1
fi

if [ `eval $CORE_CMD | wc -l` != "1" ] ; then
    echo "Not exactly one accumulo-core jar in $LIB"
    exit 1
fi

if [ `eval $THRIFT_CMD | wc -l` != "1" ] ; then
    echo "Not exactly one thrift jar in $LIB"
    exit 1
fi

if [ `eval $CLOUDTRACE_CMD | wc -l` != "1" ] ; then
    echo "Not exactly one cloudtrace jar in $LIB"
    exit 1
fi

ZOOKEEPER_LIB=`eval $ZOOKEEPER_CMD`
CORE_LIB=`eval $CORE_CMD`
THRIFT_LIB=`eval $THRIFT_CMD`
CLOUDTRACE_LIB=`eval $CLOUDTRACE_CMD`

USERJARS=" "
for arg in "$@"; do
  if [ "$arg" != "-libjars" -a -z "$TOOLJAR" ]; then
    TOOLJAR="$arg"
    shift
  elif [ "$arg" != "-libjars" -a -z "$CLASSNAME" ]; then
    CLASSNAME="$arg"
    shift
  elif [ -z "$USERJARS" ]; then
    USERJARS=`echo "$arg" | tr "," " "`
    shift
  elif [ "$arg" = "-libjars" ]; then
    USERJARS=""
    shift
  else
    break
  fi
done

LIB_JARS="$THRIFT_LIB,$CORE_LIB,$ZOOKEEPER_LIB,$CLOUDTRACE_LIB"
H_JARS="$THRIFT_LIB:$CORE_LIB:$ZOOKEEPER_LIB:$CLOUDTRACE_LIB:"

COMMONS_LIBS=`ls -1 $LIB/commons-*.jar`
for jar in $USERJARS $COMMONS_LIBS; do
  LIB_JARS="$LIB_JARS,$jar"
  H_JARS="$H_JARS$jar:"
done
export HADOOP_CLASSPATH=$H_JARS$HADOOP_CLASSPATH

if [ -z "$CLASSNAME" -o -z "$TOOLJAR" ]; then
  echo "Usage: tool.sh path/to/myTool.jar my.tool.class.Name [-libjars my1.jar,my2.jar]" 1>&2
  exit 1
fi

#echo USERJARS=$USERJARS
#echo CLASSNAME=$CLASSNAME
#echo HADOOP_CLASSPATH=$HADOOP_CLASSPATH
#echo exec "$HADOOP_HOME/bin/hadoop" jar "$TOOLJAR" $CLASSNAME -libjars \"$LIB_JARS\" $ARGS
exec "$HADOOP_HOME/bin/hadoop" jar "$TOOLJAR" $CLASSNAME -libjars \"$LIB_JARS\" "$@"
