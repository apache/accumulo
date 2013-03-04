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

hadoop dfs -rmr /testmf
USERPASS='-u root -p secret'
../../../bin/accumulo shell $USERPASS -e 'deletetable -f test_ingest'
../../../bin/accumulo org.apache.accumulo.test.TestIngest $USERPASS --rows 0 --createTable --splits 100

echo "creating first set of map files"

../../../bin/accumulo org.apache.accumulo.test.TestIngest --rfile /testmf/mf01 --timestamp 1 --size 50 --random 56 --rows 1000000 --start 0 --cols 1 &
../../../bin/accumulo org.apache.accumulo.test.TestIngest --rfile /testmf/mf02 --timestamp 1 --size 50 --random 56 --rows 1000000 --start 1000000 --cols 1 &
../../../bin/accumulo org.apache.accumulo.test.TestIngest --rfile /testmf/mf03 --timestamp 1 --size 50 --random 56 --rows 1000000 --start 2000000 --cols 1 &
../../../bin/accumulo org.apache.accumulo.test.TestIngest --rfile /testmf/mf04 --timestamp 1 --size 50 --random 56 --rows 1000000 --start 3000000 --cols 1 &
../../../bin/accumulo org.apache.accumulo.test.TestIngest --rfile /testmf/mf05 --timestamp 1 --size 50 --random 56 --rows 1000000 --start 4000000 --cols 1 &

wait

echo "bulk importing"

hadoop dfs -rmr /testmfFail
../../../bin/accumulo org.apache.accumulo.test.BulkImportDirectory $USERPASS -t test_ingest -s /testmf -f /testmfFail

echo "verifying"

../../../bin/accumulo org.apache.accumulo.test.VerifyIngest $USERPASS --size 50 --timestamp 1 --random 56 --rows 1000000 --start 0 --cols 1 &
../../../bin/accumulo org.apache.accumulo.test.VerifyIngest $USERPASS --size 50 --timestamp 1 --random 56 --rows 1000000 --start 1000000 --cols 1 &
../../../bin/accumulo org.apache.accumulo.test.VerifyIngest $USERPASS --size 50 --timestamp 1 --random 56 --rows 1000000 --start 2000000 --cols 1 &
../../../bin/accumulo org.apache.accumulo.test.VerifyIngest $USERPASS --size 50 --timestamp 1 --random 56 --rows 1000000 --start 3000000 --cols 1 &
../../../bin/accumulo org.apache.accumulo.test.VerifyIngest $USERPASS --size 50 --timestamp 1 --random 56 --rows 1000000 --start 4000000 --cols 1 &

wait

hadoop dfs -rmr /testmf

echo "creating second set of map files"

../../../bin/accumulo org.apache.accumulo.test.TestIngest --rfile /testmf/mf01 --timestamp 2 --size 50 --random 57 --rows 1000000 --start 0 --cols 1 &
../../../bin/accumulo org.apache.accumulo.test.TestIngest --rfile /testmf/mf02 --timestamp 2 --size 50 --random 57 --rows 1000000 --start 1000000 --cols 1 &
../../../bin/accumulo org.apache.accumulo.test.TestIngest --rfile /testmf/mf03 --timestamp 2 --size 50 --random 57 --rows 1000000 --start 2000000 --cols 1 &
../../../bin/accumulo org.apache.accumulo.test.TestIngest --rfile /testmf/mf04 --timestamp 2 --size 50 --random 57 --rows 1000000 --start 3000000 --cols 1 &
../../../bin/accumulo org.apache.accumulo.test.TestIngest --rfile /testmf/mf05 --timestamp 2 --size 50 --random 57 --rows 1000000 --start 4000000 --cols 1 &

wait

echo "bulk importing"

hadoop dfs -rmr /testmfFail
../../../bin/accumulo org.apache.accumulo.test.BulkImportDirectory $USERPASS -t test_ingest -s /testmf -f /testmfFail

echo "creating second set of map files"

../../../bin/accumulo org.apache.accumulo.test.VerifyIngest $USERPASS --size 50 --timestamp 2 --random 57 --rows 1000000 --start 0 --cols 1 &
../../../bin/accumulo org.apache.accumulo.test.VerifyIngest $USERPASS --size 50 --timestamp 2 --random 57 --rows 1000000 --start 1000000 --cols 1 &
../../../bin/accumulo org.apache.accumulo.test.VerifyIngest $USERPASS --size 50 --timestamp 2 --random 57 --rows 1000000 --start 2000000 --cols 1 &
../../../bin/accumulo org.apache.accumulo.test.VerifyIngest $USERPASS --size 50 --timestamp 2 --random 57 --rows 1000000 --start 3000000 --cols 1 &
../../../bin/accumulo org.apache.accumulo.test.VerifyIngest $USERPASS --size 50 --timestamp 2 --random 57 --rows 1000000 --start 4000000 --cols 1 &

