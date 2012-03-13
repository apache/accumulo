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


fail() {
   echo $@
   exit 1
}

# test to see if we have thrift installed
VERSION=`thrift -version 2>/dev/null | grep "0.6" |  wc -l`
if [ "$VERSION" -ne 1 ] ; then 
   # Nope: bail
   echo "thrift is not available"
   exit 0
fi
CLOUDTRACE=../trace
test -d ${CLOUDTRACE} || ( echo 'need to configure cloudtrace' ; exit 0 )

THRIFT_ARGS="-I $CLOUDTRACE/src/main/thrift -o target "

mkdir -p target
rm -rf target/gen-java
for f in src/main/thrift/*.thrift 
do
	thrift ${THRIFT_ARGS} --gen java $f || fail unable to generate java thrift classes
	thrift ${THRIFT_ARGS} --gen py $f || fail unable to generate python thrift classes
	thrift ${THRIFT_ARGS} --gen rb $f || fail unable to generate ruby thrift classes
done
find target/gen-java -name '*.java' -print | xargs sed -i.orig -e 's/public class /@SuppressWarnings("all") public class /'
find target/gen-java -name '*.orig' -print | xargs rm -f
# copy only files that have changed
for d in gc master tabletserver security client/impl data
do
   mkdir -p src/main/java/org/apache/accumulo/core/$d/thrift >& /dev/null
   for f in target/gen-java/org/apache/accumulo/core/$d/thrift/* 
   do
      DEST="src/main/java/org/apache/accumulo/core/$d/thrift/`basename $f`"
      if ! cmp -s ${f} ${DEST} ; then
	echo cp ${f} ${DEST} 
	cp ${f} ${DEST} || fail unable to copy files to java workspace
      fi
   done
done
