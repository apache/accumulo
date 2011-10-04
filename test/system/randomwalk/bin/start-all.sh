#!/usr/bin/env bash
bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/../../../../conf/accumulo-env.sh

if [ -z $HADOOP_HOME ] ; then
    echo "HADOOP_HOME is not set.  Please make sure it's set globally."
    exit 1
fi

if [ -z $ACCUMULO_HOME ] ; then
    echo "ACCUMULO_HOME is not set.  Please make sure it's set globally."
    exit 1
fi

if [ "$1" = "" ] ; then
     echo "Usage: start-walkers.sh <startNode>"
     exit 1
fi

RW_HOME=$ACCUMULO_HOME/test/system/randomwalk

echo 'copying randomwalk config to HDFS'
$RW_HOME/bin/copy-config.sh

echo 'starting walkers'
pssh -h $RW_HOME/conf/walkers "$RW_HOME/bin/start-local.sh $1" < /dev/null
