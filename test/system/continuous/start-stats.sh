#! /usr/bin/env bash

. continuous-env.sh

CONFIG_OUT=$CONTINUOUS_LOG_DIR/`date +%Y%m%d%H%M%S`_`hostname`_config.out

cat $ACCUMULO_HOME/conf/accumulo-env.sh > $CONFIG_OUT
echo >> $CONFIG_OUT
echo -e "config -np\nconfig -t $TABLE -np\nquit" | $ACCUMULO_HOME/bin/accumulo shell -u $USER -p $PASS >> $CONFIG_OUT
echo >> $CONFIG_OUT
cat continuous-env.sh >> $CONFIG_OUT
echo >> $CONFIG_OUT
echo "`wc -l walkers.txt`" >> $CONFIG_OUT
echo "`wc -l ingesters.txt`" >> $CONFIG_OUT
echo "`wc -l scanners.txt`" >> $CONFIG_OUT
echo "`wc -l batch_walkers.txt`" >> $CONFIG_OUT


nohup $ACCUMULO_HOME/bin/accumulo org.apache.accumulo.server.test.continuous.ContinuousStatsCollector $TABLE $INSTANCE_NAME $ZOO_KEEPERS $USER $PASS >$CONTINUOUS_LOG_DIR/`date +%Y%m%d%H%M%S`_`hostname`_stats.out 2>$CONTINUOUS_LOG_DIR/`date +%Y%m%d%H%M%S`_`hostname`_stats.err &

