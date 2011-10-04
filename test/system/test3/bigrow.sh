#! /usr/bin/env bash
../../../bin/accumulo jar ../../../lib/accumulo.jar org.apache.accumulo.server.test.TestIngest -timestamp 1 -size 50 -random 56 1 5000000 2000000;
../../../bin/accumulo jar ../../../lib/accumulo.jar org.apache.accumulo.server.test.TestIngest -timestamp 1 -size 50 -random 56 1000000 0 1;
#../../../bin/accumulo jar ../../../lib/accumulo.jar org.apache.accumulo.server.test.VerifyIngest -timestamp 1 -size 50 -random 56 1 5000000 2000000;
../../../bin/accumulo jar ../../../lib/accumulo.jar org.apache.accumulo.server.test.VerifyIngest -size 50 -timestamp 1 -random 56 1000000 0 1;
../../../bin/accumulo jar ../../../lib/accumulo.jar org.apache.accumulo.server.test.TestIngest -timestamp 1 -size 50 -random 56 1000000 7000000 1;
../../../bin/accumulo jar ../../../lib/accumulo.jar org.apache.accumulo.server.test.VerifyIngest -size 50 -timestamp 1 -random 56 1000000 7000000 1;

