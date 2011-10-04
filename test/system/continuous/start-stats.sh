#! /usr/bin/env bash

. continuous-env.sh

nohup $ACCUMULO_HOME/bin/accumulo org.apache.accumulo.server.test.continuous.ContinuousStatsCollector $TABLE $INSTANCE_NAME $ZOO_KEEPERS $USER $PASS >$CONTINUOUS_LOG_DIR/`date +%Y%m%d%H%M%S`_`hostname`_stats.out 2>$CONTINUOUS_LOG_DIR/`date +%Y%m%d%H%M%S`_`hostname`_stats.err &

