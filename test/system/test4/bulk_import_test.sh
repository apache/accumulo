#! /usr/bin/env bash
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

