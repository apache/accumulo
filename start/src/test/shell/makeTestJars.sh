#! /usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

if [ -z "$JAVA_HOME" ]; then
  echo "JAVA_HOME is not set. Java is required to proceed"
  exit 1
fi

for x in A B C; do
  mkdir -p target/generated-sources/$x/test target/test-classes/ClassLoaderTest$x
  sed "s/testX/test$x/" <src/test/java/test/TestTemplate >target/generated-sources/$x/test/TestObject.java
  export CLASSPATH=target/test-classes
  "$JAVA_HOME"/bin/javac target/generated-sources/$x/test/TestObject.java -d target/generated-sources/$x
  "$JAVA_HOME"/bin/jar -cf target/test-classes/ClassLoaderTest$x/Test.jar -C target/generated-sources/$x test/TestObject.class
  rm -r target/generated-sources/$x
done
