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


thrift0.6 -o target -gen java src/main/thrift/cloudtrace.thrift

find target/gen-java -name '*.java' -print | xargs sed -i.orig -e 's/public class /@SuppressWarnings("all") public class /'

mkdir -p src/main/java/cloudtrace/thrift
for f in target/gen-java/cloudtrace/thrift/*
do
  DEST=src/main/java/cloudtrace/thrift/`basename $f`
  if ! cmp -s $f $DEST ; then
     echo cp $f $DEST
     cp $f $DEST
  fi
done
