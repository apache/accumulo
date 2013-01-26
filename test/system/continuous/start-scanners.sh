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
AUTH_OPT="";

if [ "$DEBUG_SCANNER" = "on" ] ; then
	DEBUG_OPT="--debug $CONTINUOUS_LOG_DIR/\`date +%Y%m%d%H%M%S\`_\`hostname\`_scanner.log";
fi

if [ -n "$AUTHS" ] ; then
	AUTH_OPT="--auths \"$AUTHS\"";
fi


pssh -h scanners.txt "mkdir -p $CONTINUOUS_LOG_DIR; nohup $ACCUMULO_HOME/bin/accumulo org.apache.accumulo.test.continuous.ContinuousScanner $DEBUG_OPT $AUTH_OPT -i $INSTANCE_NAME -z $ZOO_KEEPERS -u $USER -p $PASS --table $TABLE --min $MIN --max $MAX --sleep $SCANNER_SLEEP_TIME --numToScan $SCANNER_ENTRIES >$CONTINUOUS_LOG_DIR/\`date +%Y%m%d%H%M%S\`_\`hostname\`_scanner.out 2>$CONTINUOUS_LOG_DIR/\`date +%Y%m%d%H%M%S\`_\`hostname\`_scanner.err &" < /dev/null

