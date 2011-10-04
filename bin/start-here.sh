#! /usr/bin/env bash
#
# This script starts all the accumulo services on this host
#

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

HOSTS="`hostname -a` `hostname` localhost"
for host in $HOSTS
do
    if grep -q "^${host}\$" $ACCUMULO_HOME/conf/slaves
    then
       ${bin}/start-server.sh $host logger
       ${bin}/start-server.sh $host tserver "tablet server"
       break
    fi
done

for host in $HOSTS
do
    if grep -q "^${host}\$" $ACCUMULO_HOME/conf/masters
    then
       ${bin}/accumulo org.apache.accumulo.server.master.state.SetGoalState NORMAL
       ${bin}/start-server.sh $host master
       break
    fi
done

for host in $HOSTS
do
    if [ ${host} = ${GC} ]
    then
	${bin}/start-server.sh $GC gc "garbage collector"
	break
    fi
done

for host in $HOSTS
do
    if [ ${host} = ${MONITOR} ]
    then
	${bin}/start-server.sh $MONITOR monitor 
	break
    fi
done

for host in $HOSTS
do
    if grep -q "^${host}\$" $ACCUMULO_HOME/conf/tracers
    then
	${bin}/start-server.sh $host tracer 
	break
    fi
done
