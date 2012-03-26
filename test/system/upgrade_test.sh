#!/bin/sh

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


if [ $# -ne 1 ]
then
  echo "Usage: `basename $0` clean|dirty"
  exit -1
fi

#this script test upgrade from 1.3 to 1.4.  This script is not self verifying, its output must be inspected for correctness.

#set the following to point to configured 1.3 and 1.4 accumulo dirs.  Ensure both point to the same walogs

#TODO could support multinode configs, this script assumes single node config

ONE_THREE_DIR=../../../accumulo-1.3.5-SNAPSHOT
ONE_FOUR_DIR=../../

pkill -f accumulo.start
hadoop fs -rmr /accumulo
hadoop fs -rmr /testmf
hadoop fs -rmr /testmfFail

echo "uptest\nsecret\nsecret" | $ONE_THREE_DIR/bin/accumulo init --clear-instance-name
$ONE_THREE_DIR/bin/start-all.sh
$ONE_THREE_DIR/bin/accumulo 'org.apache.accumulo.server.test.TestIngest$CreateTable' 0 200000 10 root secret
$ONE_THREE_DIR/bin/accumulo org.apache.accumulo.server.test.TestIngest -timestamp 1 -size 50 -random 56 100000 0 1
$ONE_THREE_DIR/bin/accumulo org.apache.accumulo.server.test.TestIngest -mapFile /testmf/mf01 -timestamp 1 -size 50 -random 56 100000 100000 1
$ONE_THREE_DIR/bin/accumulo org.apache.accumulo.server.test.BulkImportDirectory root secret test_ingest /testmf /testmfFail
if [ $1 == "dirty" ]; then
	pkill -9 -f accumulo.start
else 
	$ONE_THREE_DIR/bin/stop-all.sh
fi

echo "==== Starting 1.4 ==="

#Test some new 1.4 features against an upgraded instance
#TODO need to try following operations in different orders
#TODO test delete range

$ONE_FOUR_DIR/bin/start-all.sh
$ONE_FOUR_DIR/bin/accumulo  org.apache.accumulo.server.test.VerifyIngest -size 50 -timestamp 1 -random 56 200000 0 1 
echo "compact -t test_ingest -w" | $ONE_FOUR_DIR/bin/accumulo shell -u root -p secret
$ONE_FOUR_DIR/bin/accumulo  org.apache.accumulo.server.test.VerifyIngest -size 50 -timestamp 1 -random 56 200000 0 1
echo "merge -t test_ingest -s 1G" | $ONE_FOUR_DIR/bin/accumulo shell -u root -p secret
$ONE_FOUR_DIR/bin/accumulo  org.apache.accumulo.server.test.VerifyIngest -size 50 -timestamp 1 -random 56 200000 0 1
echo "clonetable test_ingest tmp\ndeletetable test_ingest\nrenametable tmp test_ingest" | $ONE_FOUR_DIR/bin/accumulo shell -u root -p secret
$ONE_FOUR_DIR/bin/accumulo  org.apache.accumulo.server.test.VerifyIngest -size 50 -timestamp 1 -random 56 200000 0 1

#test overwriting data writting in 1.3
$ONE_FOUR_DIR/bin/accumulo org.apache.accumulo.server.test.TestIngest -timestamp 2 -size 50 -random 57 300000 0 1
$ONE_FOUR_DIR/bin/accumulo  org.apache.accumulo.server.test.VerifyIngest -size 50 -timestamp 2 -random 57 300000 0 1
echo "compact -t test_ingest -w" | $ONE_FOUR_DIR/bin/accumulo shell -u root -p secret
$ONE_FOUR_DIR/bin/accumulo  org.apache.accumulo.server.test.VerifyIngest -size 50 -timestamp 2 -random 57 300000 0 1

$ONE_FOUR_DIR/bin/stop-all.sh
$ONE_FOUR_DIR/bin/start-all.sh

$ONE_FOUR_DIR/bin/accumulo  org.apache.accumulo.server.test.VerifyIngest -size 50 -timestamp 2 -random 57 300000 0 1

pkill -f accumulo.start
$ONE_FOUR_DIR/bin/start-all.sh

$ONE_FOUR_DIR/bin/accumulo  org.apache.accumulo.server.test.VerifyIngest -size 50 -timestamp 2 -random 57 300000 0 1

