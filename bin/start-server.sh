#! /usr/bin/env bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

HOST=$1
host $1 >/dev/null 2>/dev/null
if [ $? -ne 0 ]
then
  LOGHOST=$1
else
  LOGHOST="`host $1 | cut -d' ' -f1`"
fi
SERVICE="$2"
LONGNAME="$3"
if [ -z "$LONGNAME" ] 
then
   LONGNAME="$2"
fi
SLAVES=`wc -l < ${ACCUMULO_HOME}/conf/slaves`

PID=`$SSH $HOST ps -ef | egrep ${ACCUMULO_HOME}/.*/accumulo.*.jar | grep "Main $SERVICE" | grep -v grep | awk {'print $2'} | head -1`

if [ -z $PID ]; then
    echo "Starting $LONGNAME on $HOST"
    $SSH $HOST "bash -c 'exec nohup ${bin}/accumulo ${SERVICE} --address $1 >$ACCUMULO_LOG_DIR/${SERVICE}_${LOGHOST}.out 2>$ACCUMULO_LOG_DIR/${SERVICE}_${LOGHOST}.err' &" 

    MAX_FILES_OPEN=`$SSH $HOST "bash -c 'ulimit -n'"`
    if [ -n "$MAX_FILES_OPEN" ] && [ -n "$SLAVES" ] ; then
       if [ "$SLAVES" -gt 10 ] && [ "$MAX_FILES_OPEN" -lt 65536 ]; then
          echo "WARN : Max files open on $HOST is $MAX_FILES_OPEN, recommend 65536"
       fi
    fi
else
    echo "$HOST : $LONGNAME already running (${PID})"
fi
