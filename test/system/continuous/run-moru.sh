#! /usr/bin/env bash

. mapred-setup.sh

$ACCUMULO_HOME/bin/tool.sh "$SERVER_LIBJAR" org.apache.accumulo.server.test.continuous.ContinuousMoru -libjars "$SERVER_LIBJAR" $INSTANCE_NAME $ZOO_KEEPERS $USER $PASS $TABLE $MIN $MAX $MAX_CF $MAX_CQ $MAX_MEM $MAX_LATENCY $NUM_THREADS $VERIFY_MAX_MAPS

