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
mkdir -p target/generated-sources/HelloWorld/test
sed "s/%%/Hello World\!/" <src/test/java/test/HelloWorldTemplate >target/generated-sources/HelloWorld/test/HelloWorld.java
"$JAVA_HOME"/bin/javac target/generated-sources/HelloWorld/test/HelloWorld.java -d target/generated-sources/HelloWorld
"$JAVA_HOME"/bin/jar -cf target/test-classes/HelloWorld.jar -C target/generated-sources/HelloWorld test/HelloWorld.class
rm -r target/generated-sources/HelloWorld/test

mkdir -p target/generated-sources/HalloWelt/test
sed "s/%%/Hallo Welt/" <src/test/java/test/HelloWorldTemplate >target/generated-sources/HalloWelt/test/HelloWorld.java
"$JAVA_HOME"/bin/javac target/generated-sources/HalloWelt/test/HelloWorld.java -d target/generated-sources/HalloWelt
"$JAVA_HOME"/bin/jar -cf target/test-classes/HelloWorld2.jar -C target/generated-sources/HalloWelt test/HelloWorld.class
rm -r target/generated-sources/HalloWelt/test
