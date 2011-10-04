#! /usr/bin/env bash

. continuous-env.sh

DEBUG_OPT="";

if [ "$DEBUG_SCANNER" = "on" ] ; then
	DEBUG_OPT="--debug $CONTINUOUS_LOG_DIR/\`date +%Y%m%d%H%M%S\`_\`hostname\`_scanner.log";
fi

pssh -h scanners.txt "nohup $ACCUMULO_HOME/bin/accumulo org.apache.accumulo.server.test.continuous.ContinuousScanner $DEBUG_SCANNERS $INSTANCE_NAME $ZOO_KEEPERS $USER $PASS $TABLE $MIN $MAX $SCANNER_SLEEP_TIME $SCANNER_ENTRIES >$CONTINUOUS_LOG_DIR/\`date +%Y%m%d%H%M%S\`_\`hostname\`_scanner.out 2>$CONTINUOUS_LOG_DIR/\`date +%Y%m%d%H%M%S\`_\`hostname\`_scanner.err &" < /dev/null

