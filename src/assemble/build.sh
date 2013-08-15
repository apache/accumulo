#! /bin/bash

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

loc=`dirname "$0"`
loc=`cd "$loc/../.."; pwd`

cd "$loc"

fail() {
  echo '   ' $@
  exit 1
}

run() {
  echo $@
  eval $@
  if [ $? -ne 0 ]
  then
    fail $@ fails
  fi
}

runAt() {
  ( cd $1 ; echo in `pwd`; shift ; run $@ ) || fail 
}

run mvn -U -P distclean clean 
mvn org.apache.rat:apache-rat-plugin:0.9:check
COUNT=`grep '!????' target/rat.txt | wc -l`
EXPECTED=53
if [ "$COUNT" -ne $EXPECTED ]
then
   fail expected $EXPECTED files missing licenses, but saw "$COUNT"
fi
#need to run mvn package twice to properly build docs/config.html
run mvn package
run mvn package javadoc:aggregate javadoc:jar source:jar
runAt ./src/server/src/main/c++ make 
run mvn assembly:single -N
