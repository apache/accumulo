#! /usr/bin/env bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

SLAVES=$ACCUMULO_HOME/conf/slaves

echo -n "Starting tablet servers and loggers ..."

for server in `grep -v '^#' "$SLAVES"`
do 
    echo -n "."
    ${bin}/start-server.sh $server logger &
    ${bin}/start-server.sh $server tserver "tablet server" &
done

echo " done"

