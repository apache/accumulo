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

USERPASS='-u root -p secret'
../../../bin/accumulo shell $USERPASS -e 'deletetable -f test_ingest'
../../../bin/accumulo org.apache.accumulo.test.TestIngest $USERPASS --rows 0 --createTable

echo "ingesting first halves (0 to (500K - 1), 1M to (1.5M - 1), etc)"
../../../bin/accumulo org.apache.accumulo.test.TestIngest $USERPASS --timestamp 1 --size 50 --random 56 --rows 5000000 --start 0 --cols 1 &
../../../bin/accumulo org.apache.accumulo.test.TestIngest $USERPASS --timestamp 1 --size 50 --random 56 --rows 5000000 --start 1000000 --cols 1 &
../../../bin/accumulo org.apache.accumulo.test.TestIngest $USERPASS --timestamp 1 --size 50 --random 56 --rows 5000000 --start 2000000 --cols 1 &
../../../bin/accumulo org.apache.accumulo.test.TestIngest $USERPASS --timestamp 1 --size 50 --random 56 --rows 5000000 --start 3000000 --cols 1 &
../../../bin/accumulo org.apache.accumulo.test.TestIngest $USERPASS --timestamp 1 --size 50 --random 56 --rows 5000000 --start 4000000 --cols 1 &

wait

echo "ingesting second halves (500K to (1M - 1), 1.5M to (2M - 1), etc) and verifying first halves"

../../../bin/accumulo org.apache.accumulo.test.VerifyIngest $USERPASS --size 50 --timestamp 1 --random 56 --rows 5000000 --start 0 --cols 1  &
../../../bin/accumulo org.apache.accumulo.test.VerifyIngest $USERPASS --size 50 --timestamp 1 --random 56 --rows 5000000 --start 1000000 --cols 1  &
../../../bin/accumulo org.apache.accumulo.test.VerifyIngest $USERPASS --size 50 --timestamp 1 --random 56 --rows 5000000 --start 2000000 --cols 1  &
../../../bin/accumulo org.apache.accumulo.test.VerifyIngest $USERPASS --size 50 --timestamp 1 --random 56 --rows 5000000 --start 3000000 --cols 1  &
../../../bin/accumulo org.apache.accumulo.test.VerifyIngest $USERPASS --size 50 --timestamp 1 --random 56 --rows 5000000 --start 4000000 --cols 1  &

../../../bin/accumulo org.apache.accumulo.test.TestIngest $USERPASS --timestamp 1 --size 50 --random 56 --rows 5000000 --start 500000 --cols 1  &
../../../bin/accumulo org.apache.accumulo.test.TestIngest $USERPASS --timestamp 1 --size 50 --random 56 --rows 5000000 --start 1500000 --cols 1  &
../../../bin/accumulo org.apache.accumulo.test.TestIngest $USERPASS --timestamp 1 --size 50 --random 56 --rows 5000000 --start 2500000 --cols 1  &
../../../bin/accumulo org.apache.accumulo.test.TestIngest $USERPASS --timestamp 1 --size 50 --random 56 --rows 5000000 --start 3500000 --cols 1  &
../../../bin/accumulo org.apache.accumulo.test.TestIngest $USERPASS --timestamp 1 --size 50 --random 56 --rows 5000000 --start 4500000 --cols 1  &

wait

echo "verifying complete range"

../../../bin/accumulo org.apache.accumulo.test.VerifyIngest $USERPASS --size 50 --timestamp 1 --random 56 --rows 1000000 --start 0 --cols 1  &
../../../bin/accumulo org.apache.accumulo.test.VerifyIngest $USERPASS --size 50 --timestamp 1 --random 56 --rows 1000000 --start 1000000 --cols 1  &
../../../bin/accumulo org.apache.accumulo.test.VerifyIngest $USERPASS --size 50 --timestamp 1 --random 56 --rows 1000000 --start 2000000 --cols 1  &
../../../bin/accumulo org.apache.accumulo.test.VerifyIngest $USERPASS --size 50 --timestamp 1 --random 56 --rows 1000000 --start 3000000 --cols 1  &
../../../bin/accumulo org.apache.accumulo.test.VerifyIngest $USERPASS --size 50 --timestamp 1 --random 56 --rows 1000000 --start 4000000 --cols 1  &

wait

echo "ingesting first halves (0 to (500K - 1), 1M to (1.5M - 1), etc) w/ new timestamp AND verifying second half w/ old timestamp"

../../../bin/accumulo org.apache.accumulo.test.TestIngest $USERPASS --timestamp 2 --size 50 --random 57 --rows 500000 --start 0 --cols 1  &
../../../bin/accumulo org.apache.accumulo.test.TestIngest $USERPASS --timestamp 2 --size 50 --random 57 --rows 500000 --start 1000000 --cols 1  &
../../../bin/accumulo org.apache.accumulo.test.TestIngest $USERPASS --timestamp 2 --size 50 --random 57 --rows 500000 --start 2000000 --cols 1  &
../../../bin/accumulo org.apache.accumulo.test.TestIngest $USERPASS --timestamp 2 --size 50 --random 57 --rows 500000 --start 3000000 --cols 1  &
../../../bin/accumulo org.apache.accumulo.test.TestIngest $USERPASS --timestamp 2 --size 50 --random 57 --rows 500000 --start 4000000 --cols 1  &

../../../bin/accumulo org.apache.accumulo.test.VerifyIngest $USERPASS --size 50 --timestamp 1 --random 56 --rows 500000 --start 500000 --cols 1  &
../../../bin/accumulo org.apache.accumulo.test.VerifyIngest $USERPASS --size 50 --timestamp 1 --random 56 --rows 500000 --start 1500000 --cols 1  &
../../../bin/accumulo org.apache.accumulo.test.VerifyIngest $USERPASS --size 50 --timestamp 1 --random 56 --rows 500000 --start 2500000 --cols 1  &
../../../bin/accumulo org.apache.accumulo.test.VerifyIngest $USERPASS --size 50 --timestamp 1 --random 56 --rows 500000 --start 3500000 --cols 1  &
../../../bin/accumulo org.apache.accumulo.test.VerifyIngest $USERPASS --size 50 --timestamp 1 --random 56 --rows 500000 --start 4500000 --cols 1  &


wait

echo "ingesting second halves (500K to (1M - 1), 1.5M to (2M - 1), etc) and verifying first halves"

../../../bin/accumulo org.apache.accumulo.test.VerifyIngest $USERPASS --size 50 --timestamp 2 --random 57 --rows 500000 --start 0 --cols 1  &
../../../bin/accumulo org.apache.accumulo.test.VerifyIngest $USERPASS --size 50 --timestamp 2 --random 57 --rows 500000 --start 1000000 --cols 1  &
../../../bin/accumulo org.apache.accumulo.test.VerifyIngest $USERPASS --size 50 --timestamp 2 --random 57 --rows 500000 --start 2000000 --cols 1  &
../../../bin/accumulo org.apache.accumulo.test.VerifyIngest $USERPASS --size 50 --timestamp 2 --random 57 --rows 500000 --start 3000000 --cols 1  &
../../../bin/accumulo org.apache.accumulo.test.VerifyIngest $USERPASS --size 50 --timestamp 2 --random 57 --rows 500000 --start 4000000 --cols 1  &

../../../bin/accumulo org.apache.accumulo.test.TestIngest $USERPASS --timestamp 2 --size 50 --random 57 --rows 500000 --start 500000 --cols 1  &
../../../bin/accumulo org.apache.accumulo.test.TestIngest $USERPASS --timestamp 2 --size 50 --random 57 --rows 500000 --start 1500000 --cols 1  &
../../../bin/accumulo org.apache.accumulo.test.TestIngest $USERPASS --timestamp 2 --size 50 --random 57 --rows 500000 --start 2500000 --cols 1  &
../../../bin/accumulo org.apache.accumulo.test.TestIngest $USERPASS --timestamp 2 --size 50 --random 57 --rows 500000 --start 3500000 --cols 1  &
../../../bin/accumulo org.apache.accumulo.test.TestIngest $USERPASS --timestamp 2 --size 50 --random 57 --rows 500000 --start 4500000 --cols 1  &

wait

echo "verifying complete range"

../../../bin/accumulo org.apache.accumulo.test.VerifyIngest $USERPASS --size 50 --timestamp 2 --random 57 --rows 1000000 --start 0 --cols 1  &
../../../bin/accumulo org.apache.accumulo.test.VerifyIngest $USERPASS --size 50 --timestamp 2 --random 57 --rows 1000000 --start 1000000 --cols 1  &
../../../bin/accumulo org.apache.accumulo.test.VerifyIngest $USERPASS --size 50 --timestamp 2 --random 57 --rows 1000000 --start 2000000 --cols 1  &
../../../bin/accumulo org.apache.accumulo.test.VerifyIngest $USERPASS --size 50 --timestamp 2 --random 57 --rows 1000000 --start 3000000 --cols 1  &
../../../bin/accumulo org.apache.accumulo.test.VerifyIngest $USERPASS --size 50 --timestamp 2 --random 57 --rows 1000000 --start 4000000 --cols 1  &


wait
