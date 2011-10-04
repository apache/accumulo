#! /usr/bin/env bash

. continuous-env.sh

nohup ./agitator.pl $KILL_SLEEP_TIME $TUP_SLEEP_TIME $MIN_KILL $MAX_KILL >$CONTINUOUS_LOG_DIR/`date +%Y%m%d%H%M%S`_`hostname`_agitator.out 2>$CONTINUOUS_LOG_DIR/`date +%Y%m%d%H%M%S`_`hostname`_agitator.err &

nohup ./magitator.pl $MASTER_KILL_SLEEP_TIME $MASTER_RESTART_SLEEP_TIME >$CONTINUOUS_LOG_DIR/`date +%Y%m%d%H%M%S`_`hostname`_magitator.out 2>$CONTINUOUS_LOG_DIR/`date +%Y%m%d%H%M%S`_`hostname`_magitator.err &
