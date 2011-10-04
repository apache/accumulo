#! /usr/bin/env bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

SLAVES=$ACCUMULO_HOME/conf/slaves

echo -n "Starting tablet servers and loggers ..."

count=1
for server in `grep -v '^#' "$SLAVES"`
do 
    echo -n "."
    ${bin}/start-server.sh $server logger &
    ${bin}/start-server.sh $server tserver "tablet server" &
    count=`expr $count + 1`
    if [ `expr $count % 72` -eq 0 ] ;
    then
       echo
       wait
    fi
done

echo " done"

