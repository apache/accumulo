#!/usr/bin/env bash

if [ -z $HADOOP_HOME ] ; then
    echo "HADOOP_HOME is not set.  Please make sure it's set globally."
    exit 1
fi

if [ -z $ACCUMULO_HOME ] ; then
    echo "ACCUMULO_HOME is not set.  Please make sure it's set globally."
    exit 1
fi

RW_HOME=$ACCUMULO_HOME/test/system/randomwalk

cd $RW_HOME

tar czf config.tgz conf
$HADOOP_HOME/bin/hadoop fs -rmr /randomwalk
$HADOOP_HOME/bin/hadoop fs -mkdir /randomwalk
$HADOOP_HOME/bin/hadoop fs -put config.tgz /randomwalk
$HADOOP_HOME/bin/hadoop fs -setrep 3 /randomwalk/config.tgz
rm config.tgz
