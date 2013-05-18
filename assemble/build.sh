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
loc=`cd "$loc/.."; pwd`

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

cacheGPG() {
  # make sure gpg agent has key cached
  # TODO prompt for key instead of using default?
  TESTFILE="/tmp/${USER}-gpgTestFile-$(date -u +%s).txt"
  touch "${TESTFILE}" && gpg --sign "${TESTFILE}" && rm -f "${TESTFILE}" "${TESTFILE}.gpg"
}

setupRPM() {
  # if you want the RPM signed, copy contrib/dotfile-rpmmacros to $HOME/.rpmmacros
  diff -q "contrib/dotfile-rpmmacros" "$HOME/.rpmmacros"
  R=$?
  if [[ $R = 0 ]]; then
    true
  elif [[ $R = 1 ]]; then
    run mv -n "$HOME/.rpmmacros" "$HOME/.rpmmacros-bak-$(date -u +%s)"
    run cp "contrib/dotfile-rpmmacros" "$HOME/.rpmmacros"
  elif [[ ! -r "$HOME/.rpmmacros" ]]; then
    run cp "contrib/dotfile-rpmmacros" "$HOME/.rpmmacros"
  else
    fail diff returned $R
  fi
}

if [[ $1 = '--create-release-candidate' ]]; then
  cacheGPG; setupRPM
  # create a release candidate from a branch
  run mvn clean release:clean release:prepare release:perform
elif [[ $1 = '--seal-jars' ]]; then
  cacheGPG; setupRPM
  # build a tag, but with sealed jars
  run mvn clean compile javadoc:aggregate install \
   -P apache-release,seal-jars,check-licenses,thrift,native,assemble,docs,rpm,deb
elif [[ $1 = '--test' ]]; then
  cacheGPG; setupRPM
  # build a tag, but with tests
  run mvn clean compile javadoc:aggregate install \
   -P apache-release,check-licenses,thrift,native,assemble,docs,rpm,deb
else
  fail "Missing one of: --create-release-candidate, --test, --seal-jars"
fi

