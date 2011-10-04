#!/usr/bin/env bash

if [ -z $HADOOP_HOME ] ; then
    echo "HADOOP_HOME is not set.  Please make sure it's set globally."
    exit 1
fi

if [ -z $ACCUMULO_HOME ] ; then
    echo "ACCUMULO_HOME is not set.  Please make sure it's set globally."
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
