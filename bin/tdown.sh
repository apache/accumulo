#! /usr/bin/env bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

HADOOP_CMD=$HADOOP_LOCATION/bin/hadoop

SLAVES=$ACCUMULO_HOME/conf/slaves

echo 'stopping unresponsive tablet servers (if any) ...'
for server in `cat $SLAVES | grep -v '^#' `; do
        # only start if there's not one already running
        $ACCUMULO_HOME/bin/stop-server.sh $server "$ACCUMULO_HOME/.*/accumulo-start.*.jar" tserver TERM & 
        $ACCUMULO_HOME/bin/stop-server.sh $server "$ACCUMULO_HOME/.*/accumulo-start.*.jar" logger TERM & 
done

sleep 10

echo 'stopping unresponsive tablet servers hard (if any) ...'
for server in `cat $SLAVES | grep -v '^#' `; do
        # only start if there's not one already running
        $ACCUMULO_HOME/bin/stop-server.sh $server "$ACCUMULO_HOME/.*/accumulo-start.*.jar" tserver KILL & 
        $ACCUMULO_HOME/bin/stop-server.sh $server "$ACCUMULO_HOME/.*/accumulo-start.*.jar" logger KILL & 
done

echo 'Cleaning tablet server and logger entries from zookeeper'
$ACCUMULO_HOME/bin/accumulo org.apache.accumulo.server.util.ZooZap -tservers -loggers
