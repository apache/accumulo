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

# This script will regenerate the thrift code for cloudtrace. This should be run at least whenever the thrift code is changed. There is no harm, besides wasted
# cycles, from running it more often then that.

# Generates the cloudtrace thrift code. We're explicitly using thrift0.6 because other versions
# are not compatible
thrift0.6 -o target -gen java src/main/thrift/cloudtrace.thrift

# For all generated thrift code, suppress all warnings
find target/gen-java -name '*.java' -print | xargs sed -i.orig -e 's/public class /@SuppressWarnings("all") public class /'

# Make a directory for said thrift code if does not already exist
mkdir -p src/main/java/cloudtrace/thrift

# For every file, move it with the appropriate path name IFF they are different
for f in target/gen-java/cloudtrace/thrift/*
do
  DEST=src/main/java/cloudtrace/thrift/`basename $f`
  if ! cmp -s $f $DEST ; then
     echo cp $f $DEST
     cp $f $DEST
  fi
done
