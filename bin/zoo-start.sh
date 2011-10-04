#! /usr/bin/env bash
bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh
cd "$ACCUMULO_HOME"

if [ -z "$ZOOKEEPER_HOME" ]
then
   ZK=`which zkServer.sh 2>/dev/null`
   if [ -z "$ZK" ]
   then
       echo ZOOKEEPER_HOME not set 1>&2
       exit 1
   fi
   ZK=`dirname $ZK`
   ZOOKEEPER_HOME=`dirname $ZK`
fi

ZOOKEEPER_ZOOCFG="$ZOOKEEPER_HOME/conf/zoo.cfg";
ZOOKEEPER_BIN="$ZOOKEEPER_HOME/bin"

SERVERS=`grep ^server "$ZOOKEEPER_ZOOCFG" | cut -d= -f2 | cut -d: -f1 | wc -l`
if [ "$SERVERS" -eq 0 ] 
then
   echo 'Starting stand-alone zookeeper'
   $ZOOKEEPER_HOME/bin/zkServer.sh start
else
    for server in `grep ^server "$ZOOKEEPER_ZOOCFG" | cut -d= -f2 | cut -d: -f1`
    do
      echo "Starting $server ..."
      ssh -qnf $server "nohup $ZOOKEEPER_BIN/zkServer.sh start &" &
    done
fi

