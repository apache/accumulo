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

if [ -f $ACCUMULO_HOME/conf/accumulo-env.sh ] ; then
. $ACCUMULO_HOME/conf/accumulo-env.sh
fi

if [ -z $HADOOP_HOME ] ; then
    echo "HADOOP_HOME is not set.  Please make sure it's set globally."
    exit 1
fi

if [ "$1" = "" ] ; then
     echo "Usage: update-cluster.sh <TARFILE>"
     exit 1
fi

echo 'killing accumulo'
pssh -h $ACCUMULO_HOME/conf/slaves "pkill -f org.apache.accumulo.start" < /dev/null
pkill -f org.apache.accumulo.start
pkill -f agitator.pl

echo 'updating accumulo'
cd $ACCUMULO_HOME/..
tar xzf $1

echo 'cleaning logs directory'
rm -f $ACCUMULO_HOME/logs/*
rm -f $ACCUMULO_HOME/test/system/randomwalk/logs/*
rm -f $ACCUMULO_HOME/test/system/continuous/logs/*
rm -f ~/rwlogs/*

echo 'removing old code'
pssh -h $ACCUMULO_HOME/conf/slaves "rm -rf $ACCUMULO_HOME" < /dev/null

echo 'pushing new code'
prsync -r -h $ACCUMULO_HOME/conf/slaves $ACCUMULO_HOME /opt/dev

echo 'removing /accumulo dir'
$HADOOP_HOME/bin/hadoop fs -rmr /accumulo

echo 'creating new instance'
printf "test\nY\nsecret\nsecret\n" | $ACCUMULO_HOME/bin/accumulo init

echo 'starting accumulo'
$ACCUMULO_HOME/bin/start-all.sh
