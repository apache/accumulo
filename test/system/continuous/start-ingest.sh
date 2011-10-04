#! /usr/bin/env bash

. continuous-env.sh

DEBUG_OPT="";

if [ "$DEBUG_INGEST" = "on" ] ; then
	DEBUG_OPT="--debug $CONTINUOUS_LOG_DIR/\`date +%Y%m%d%H%M%S\`_\`hostname\`_ingest.log";
fi

pssh -h ingesters.txt "nohup $ACCUMULO_HOME/bin/accumulo org.apache.accumulo.server.test.continuous.ContinuousIngest $DEBUG_OPT $INSTANCE_NAME $ZOO_KEEPERS $USER $PASS $TABLE $MIN $MAX $MAX_CF $MAX_CQ $MAX_MEM $MAX_LATENCY $NUM_THREADS >$CONTINUOUS_LOG_DIR/\`date +%Y%m%d%H%M%S\`_\`hostname\`_ingest.out 2>$CONTINUOUS_LOG_DIR/\`date +%Y%m%d%H%M%S\`_\`hostname\`_ingest.err &" < /dev/null

