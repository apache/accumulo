#! /usr/bin/env bash
#
# This script safely stops all the accumulo services on this host
#

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

START="$ACCUMULO_HOME/.*/accumulo-start.*.jar"

if grep -q localhost $ACCUMULO_HOME/conf/slaves
then
    $bin/accumulo admin stop localhost
else
    $bin/accumulo admin stop `hostname -a`
fi

for signal in TERM KILL
do
    for svc in tserver gc master monitor logger tracer
    do
	PID=`ps -ef | egrep ${START} | grep "Main $svc" | grep -v grep | grep -v stop-here.sh | awk {'print \$2'} | head -1`
	if [ ! -z $PID ]; then
	    echo "stopping $svc on `hostname -a` with signal $signal"
	    kill -s ${signal} ${PID}
	fi
    done
done
