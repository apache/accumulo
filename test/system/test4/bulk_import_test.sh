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

echo "creating first set of map files"

../../../bin/accumulo org.apache.accumulo.server.test.TestIngest -mapFile /testmf/mf01 -timestamp 1 -size 50 -random 56 1000000 0 1 &
../../../bin/accumulo org.apache.accumulo.server.test.TestIngest -mapFile /testmf/mf02 -timestamp 1 -size 50 -random 56 1000000 1000000 1 &
../../../bin/accumulo org.apache.accumulo.server.test.TestIngest -mapFile /testmf/mf03 -timestamp 1 -size 50 -random 56 1000000 2000000 1 &
../../../bin/accumulo org.apache.accumulo.server.test.TestIngest -mapFile /testmf/mf04 -timestamp 1 -size 50 -random 56 1000000 3000000 1 &
../../../bin/accumulo org.apache.accumulo.server.test.TestIngest -mapFile /testmf/mf05 -timestamp 1 -size 50 -random 56 1000000 4000000 1 &

wait

echo "bulk importing"

hadoop dfs -rmr /testmfFail
../../../bin/accumulo org.apache.accumulo.server.test.BulkImportDirectory root secret test_ingest /testmf /testmfFail

echo "verifying"

../../../bin/accumulo org.apache.accumulo.server.test.VerifyIngest -size 50 -timestamp 1 -random 56 1000000 0 1 &
../../../bin/accumulo org.apache.accumulo.server.test.VerifyIngest -size 50 -timestamp 1 -random 56 1000000 1000000 1 &
../../../bin/accumulo org.apache.accumulo.server.test.VerifyIngest -size 50 -timestamp 1 -random 56 1000000 2000000 1 &
../../../bin/accumulo org.apache.accumulo.server.test.VerifyIngest -size 50 -timestamp 1 -random 56 1000000 3000000 1 &
../../../bin/accumulo org.apache.accumulo.server.test.VerifyIngest -size 50 -timestamp 1 -random 56 1000000 4000000 1 &

wait

hadoop dfs -rmr /testmf

echo "creating second set of map files"

../../../bin/accumulo org.apache.accumulo.server.test.TestIngest -mapFile /testmf/mf01 -timestamp 2 -size 50 -random 57 1000000 0 1 &
../../../bin/accumulo org.apache.accumulo.server.test.TestIngest -mapFile /testmf/mf02 -timestamp 2 -size 50 -random 57 1000000 1000000 1 &
../../../bin/accumulo org.apache.accumulo.server.test.TestIngest -mapFile /testmf/mf03 -timestamp 2 -size 50 -random 57 1000000 2000000 1 &
../../../bin/accumulo org.apache.accumulo.server.test.TestIngest -mapFile /testmf/mf04 -timestamp 2 -size 50 -random 57 1000000 3000000 1 &
../../../bin/accumulo org.apache.accumulo.server.test.TestIngest -mapFile /testmf/mf05 -timestamp 2 -size 50 -random 57 1000000 4000000 1 &

wait

echo "bulk importing"

hadoop dfs -rmr /testmfFail
../../../bin/accumulo org.apache.accumulo.server.test.BulkImportDirectory root secret test_ingest /testmf /testmfFail

echo "creating second set of map files"

../../../bin/accumulo org.apache.accumulo.server.test.VerifyIngest -size 50 -timestamp 2 -random 57 1000000 0 1 &
../../../bin/accumulo org.apache.accumulo.server.test.VerifyIngest -size 50 -timestamp 2 -random 57 1000000 1000000 1 &
../../../bin/accumulo org.apache.accumulo.server.test.VerifyIngest -size 50 -timestamp 2 -random 57 1000000 2000000 1 &
../../../bin/accumulo org.apache.accumulo.server.test.VerifyIngest -size 50 -timestamp 2 -random 57 1000000 3000000 1 &
../../../bin/accumulo org.apache.accumulo.server.test.VerifyIngest -size 50 -timestamp 2 -random 57 1000000 4000000 1 &

