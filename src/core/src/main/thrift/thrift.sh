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


# test to see if we have thrift installed
VERSION=`thrift -version 2>/dev/null | grep "0.3" |	wc -l`
if [ "$VERSION" -ne 1 ] ; then 
	# Nope: bail
	echo "thrift is not available"
	exit 0
fi
CLOUDTRACE=../trace
THRIFT_ARGS="-I $CLOUDTRACE/src/main/thrift -o target "

mkdir -p target
for f in src/main/thrift/*.thrift 
do
	thrift ${THRIFT_ARGS} --gen java $f
	thrift ${THRIFT_ARGS} --gen py $f
	thrift ${THRIFT_ARGS} --gen rb $f
done
# copy only files that have changed
for d in gc master tabletserver security client/impl data
do
	mkdir -p src/main/java/org/apache/accumulo/core/$d/thrift >& /dev/null
	for f in target/gen-java/org/apache/accumulo/core/$d/thrift/* 
	do
		DEST="src/main/java/org/apache/accumulo/core/$d/thrift/`basename $f`"
		if ! cmp -s ${f} ${DEST} ; then
			echo cp ${f} ${DEST} 
			cp ${f} ${DEST} 
		fi
	done
done