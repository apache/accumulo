#! /usr/bin/env bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh
unset DISPLAY

if [ -z $ZOOKEEPER_HOME ] ; then
    echo "ZOOKEEPER_HOME is not set.  Please make sure it's set globally or in conf/accumulo-env.sh"
    exit 1
fi

ZOOKEEPER_VERSION=`(cd $ZOOKEEPER_HOME; ls zookeeper-[0-9]*.jar | head -1)`
ZOOKEEPER_VERSION=${ZOOKEEPER_VERSION/zookeeper-/}
ZOOKEEPER_VERSION=${ZOOKEEPER_VERSION/.jar/}

if [ "$ZOOKEEPER_VERSION" '<' "3.3.0" ] ; then 
	echo "WARN : Using Zookeeper $ZOOKEEPER_VERSION.  Use version 3.3.0 or greater to avoid zookeeper deadlock bug.";
fi

if [ "$1" != "--notSlaves" ] ; then
	${bin}/tup.sh
fi

${bin}/accumulo org.apache.accumulo.server.master.state.SetGoalState NORMAL
for master in `grep -v '^#' "$ACCUMULO_HOME/conf/masters"`
do
    ${bin}/start-server.sh $master master
done

${bin}/start-server.sh $GC gc "garbage collector"

${bin}/start-server.sh $MONITOR monitor 

for tracer in `grep -v '^#' "$ACCUMULO_HOME/conf/tracers"`
do
   ${bin}/start-server.sh $tracer tracer
done
