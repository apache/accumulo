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
     echo "Usage: start-local.sh <startNode>"
     exit 1
fi

RW_HOME=$ACCUMULO_HOME/test/system/randomwalk

cd $RW_HOME

# grab config from HDFS
$HADOOP_HOME/bin/hadoop fs -get /randomwalk/config.tgz config.tgz

# extract config to a tmp directory
rm -rf tmp/
mkdir tmp/
tar xzf config.tgz -C tmp/
rm config.tgz

# config the logging
RW_LOGS=$RW_HOME/logs
LOG_ID=`hostname -s`_`date +%Y%m%d_%H%M%S`

# run the local walker
$ACCUMULO_HOME/bin/accumulo org.apache.accumulo.server.test.randomwalk.Framework $RW_HOME/tmp/conf/ $RW_LOGS $LOG_ID $1 >$RW_LOGS/$LOG_ID.out 2>$RW_LOGS/$LOG_ID.err &
