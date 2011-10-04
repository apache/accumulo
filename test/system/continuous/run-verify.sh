#! /usr/bin/env bash

. mapred-setup.sh

$ACCUMULO_HOME/bin/tool.sh "$SERVER_LIBJAR" org.apache.accumulo.server.test.continuous.ContinuousVerify -libjars "$SERVER_LIBJAR" $INSTANCE_NAME $ZOO_KEEPERS $USER $PASS $TABLE $VERFIY_OUT $VERIFY_MAX_MAPS $VERIFY_REDUCERS

