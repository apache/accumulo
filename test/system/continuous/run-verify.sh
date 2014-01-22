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


. mapred-setup.sh

AUTH_OPT="";

if [ -n "$VERIFY_AUTHS" ] ; then
	AUTH_OPT="--auths $VERIFY_AUTHS";
fi

if [ ! -r $ACCUMULO_CONF_DIR/accumulo-site.xml ]; then
    echo "Could not find accumulo-site.xml in $ACCUMULO_CONF_DIR"
    exit 1
fi

TARGET_DIR="ci-conf-`date '+%s'`"
hadoop fs -mkdir $TARGET_DIR

if [ $? -ne 0 ]; then
    echo "Could not create $TAGET_DIR in HDFS"
    exit 1
fi

hadoop fs -put $ACCUMULO_CONF_DIR/accumulo-site.xml ${TARGET_DIR}/

if [ $? -ne 0 ]; then
    echo "Could not upload accumulo-site.xml to HDFS"
    exit 1
fi

ABS_DIR="/user/`whoami`/${TARGET_DIR}/accumulo-site.xml"

$ACCUMULO_HOME/bin/tool.sh "$SERVER_LIBJAR" org.apache.accumulo.server.test.continuous.ContinuousVerify -libjars "$SERVER_LIBJAR" $AUTH_OPT $INSTANCE_NAME $ZOO_KEEPERS $USER $PASS $TABLE $VERIFY_OUT $VERIFY_MAX_MAPS $VERIFY_REDUCERS --sitefile $ABS_DIR  $SCAN_OFFLINE

