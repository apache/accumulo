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


if [[ $# != 1 ]] ; then
  BASENAME=$(basename "$0")
  echo "Usage: $BASENAME clean|dirty"
  exit -1
fi

#this script test upgrade.   This script is not self verifying, its output must be inspected for correctness.

#set DIR  to point to configured accumulo dirs. 

#TODO could support multinode configs, this script assumes single node config

PREV=../../../../accumulo-1.5.0
CURR=../../
DIR=/accumulo
BULK=/tmp/upt

pkill -f accumulo.start
hadoop fs -rmr "$DIR"
hadoop fs -rmr "$BULK"
hadoop fs -mkdir "$BULK/fail"

"$PREV/bin/accumulo" init --clear-instance-name --instance-name testUp --password secret
"$PREV/bin/start-all.sh"

"$PREV/bin/accumulo" org.apache.accumulo.test.TestIngest -u root -p secret --timestamp 1 --size 50 --random 56 --rows 200000 --start 0 --cols 1  --createTable --splits 10
"$PREV/bin/accumulo" org.apache.accumulo.test.TestIngest --rfile $BULK/bulk/test --timestamp 1 --size 50 --random 56 --rows 200000 --start 200000 --cols 1

echo -e "table test_ingest\nimportdirectory $BULK/bulk $BULK/fail false" | $PREV/bin/accumulo shell -u root -p secret
if [[ $1 == dirty ]]; then
	pkill -9 -f accumulo.start
else 
	"$PREV/bin/stop-all.sh"
fi

echo "==== Starting Current ==="

"$CURR/bin/start-all.sh"
"$CURR/bin/accumulo" org.apache.accumulo.test.VerifyIngest --size 50 --timestamp 1 --random 56 --rows 400000 --start 0 --cols 1 -u root -p secret
echo "compact -t test_ingest -w" | $CURR/bin/accumulo shell -u root -p secret
"$CURR/bin/accumulo" org.apache.accumulo.test.VerifyIngest --size 50 --timestamp 1 --random 56 --rows 400000 --start 0 --cols 1 -u root -p secret


"$CURR/bin/accumulo" org.apache.accumulo.test.TestIngest --timestamp 2 --size 50 --random 57 --rows 500000 --start 0 --cols 1 -u root -p secret
"$CURR/bin/accumulo" org.apache.accumulo.test.VerifyIngest --size 50 --timestamp 2 --random 57 --rows 500000 --start 0 --cols 1 -u root -p secret
echo "compact -t test_ingest -w" | $CURR/bin/accumulo shell -u root -p secret
"$CURR/bin/accumulo" org.apache.accumulo.test.VerifyIngest --size 50 --timestamp 2 --random 57 --rows 500000 --start 0 --cols 1 -u root -p secret

"$CURR/bin/stop-all.sh"
"$CURR/bin/start-all.sh"

"$CURR/bin/accumulo" org.apache.accumulo.test.VerifyIngest --size 50 --timestamp 2 --random 57 --rows 500000 --start 0 --cols 1 -u root -p secret

pkill -9 -f accumulo.start
"$CURR/bin/start-all.sh"

"$CURR/bin/accumulo" org.apache.accumulo.test.VerifyIngest --size 50 --timestamp 2 --random 57 --rows 500000 --start 0 --cols 1 -u root -p secret

