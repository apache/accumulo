#! /usr/bin/env bash

. continuous-env.sh

DEBUG_OPT="";

if [ "$DEBUG_BATCH_WALKER" = "on" ] ; then
	DEBUG_OPT="--debug $CONTINUOUS_LOG_DIR/\`date +%Y%m%d%H%M%S\`_\`hostname\`_batch_walk.log";
fi

pssh -h batch_walkers.txt "nohup $ACCUMULO_HOME/bin/accumulo org.apache.accumulo.server.test.continuous.ContinuousBatchWalker $DEBUG_OPT $INSTANCE_NAME $ZOO_KEEPERS $USER $PASS $TABLE $MIN $MAX $BATCH_WALKER_SLEEP $BATCH_WALKER_BATCH_SIZE $BATCH_WALKER_THREADS >$CONTINUOUS_LOG_DIR/\`date +%Y%m%d%H%M%S\`_\`hostname\`_batch_walk.out 2>$CONTINUOUS_LOG_DIR/\`date +%Y%m%d%H%M%S\`_\`hostname\`_batch_walk.err &" < /dev/null

