#! /usr/bin/env bash

. continuous-env.sh

DEBUG_OPT="";

if [ "$DEBUG_WALKER" = "on" ] ; then
	DEBUG_OPT="--debug $CONTINUOUS_LOG_DIR/\`date +%Y%m%d%H%M%S\`_\`hostname\`_walk.log";
fi

pssh -h walkers.txt "nohup $ACCUMULO_HOME/bin/accumulo org.apache.accumulo.server.test.continuous.ContinuousWalk $DEBUG_OPT $INSTANCE_NAME $ZOO_KEEPERS $USER $PASS $TABLE $MIN $MAX $SLEEP_TIME >$CONTINUOUS_LOG_DIR/\`date +%Y%m%d%H%M%S\`_\`hostname\`_walk.out 2>$CONTINUOUS_LOG_DIR/\`date +%Y%m%d%H%M%S\`_\`hostname\`_walk.err &" < /dev/null

