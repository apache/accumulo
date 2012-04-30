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

LOCAL_MAVEN_REPO=$1

if [ "null" = $LOCAL_MAVEN_REPO ] ; then
  LOCAL_MAVEN_REPO=
elif [ ! -z $LOCAL_MAVEN_REPO ] ; then
  LOCAL_MAVEN_REPO="-Dmaven.repo.local=$LOCAL_MAVEN_REPO"
fi

run svn export https://svn.apache.org/repos/asf/thrift/tags/thrift-0.3.0
runAt thrift-0.3.0/lib/java ant
run mvn install:install-file $LOCAL_MAVEN_REPO -Dfile=thrift-0.3.0/lib/java/libthrift.jar -DgroupId=org.apache.accumulo.thrift -DartifactId=libthrift -Dversion=0.3 -Dpackaging=jar
run rm -rf thrift-0.3.0
