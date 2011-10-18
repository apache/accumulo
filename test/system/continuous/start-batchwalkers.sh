#! /usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


. continuous-env.sh

DEBUG_OPT="";

if [ "$DEBUG_BATCH_WALKER" = "on" ] ; then
	DEBUG_OPT="--debug $CONTINUOUS_LOG_DIR/\`date +%Y%m%d%H%M%S\`_\`hostname\`_batch_walk.log";
fi

pssh -h batch_walkers.txt "nohup $ACCUMULO_HOME/bin/accumulo org.apache.accumulo.server.test.continuous.ContinuousBatchWalker $DEBUG_OPT $INSTANCE_NAME $ZOO_KEEPERS $USER $PASS $TABLE $MIN $MAX $BATCH_WALKER_SLEEP $BATCH_WALKER_BATCH_SIZE $BATCH_WALKER_THREADS >$CONTINUOUS_LOG_DIR/\`date +%Y%m%d%H%M%S\`_\`hostname\`_batch_walk.out 2>$CONTINUOUS_LOG_DIR/\`date +%Y%m%d%H%M%S\`_\`hostname\`_batch_walk.err &" < /dev/null

