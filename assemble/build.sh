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

# Allow skipping tests; This option is not possible
# with the --create-release-candidate option
if [[ $1 != '--skipTests' ]]; then
  # Run all tests with Hadoop 1.0.x
  run mvn clean
  run mvn verify

  # Run all tests with Hadoop 2.0.x
  run mvn clean
  run mvn verify -Dhadoop.profile=2.0
fi

# Build and stage release artifacts; dryRun is assumed, unless
# this script is executed with --create-release-candidate flag.
DRYRUN='-DdryRun'
if [[ $1 = '--create-release-candidate' ]]; then
  DRYRUN=''
fi

# make sure gpg agent has key cached; TODO prompt for key instead of using default
# if you want the RPM signed, copy contrib/dotfiles-rpmmacros to $HOME/.rpmmacros
TESTFILE=/tmp/${USER}-gpgTestFile.txt
touch "${TESTFILE}" && gpg --sign "${TESTFILE}" && rm -f "${TESTFILE}" "${TESTFILE}.gpg"

run mvn clean release:clean release:prepare $DRYRUN
run mvn release:perform $DRYRUN

if [[ $DRYRUN = '-DdryRun' ]]; then
  echo '**************************************************'
  echo '  You performed a dryRun release. To tag and'
  echo '  stage a real release candidate for a vote,'
  echo '  execute this script as:'
  echo "    $0 --create-release-candidate"
  echo '**************************************************'
else
  echo '**************************************************'
  echo '  You\'ve successfully tagged and staged a'
  echo '  release candidate. If the vote succeeds, rename'
  echo '  the release candidate to its final name,'
  echo '  promote the staging repository, copy the'
  echo '  artifacts to the dist svn, update the web page,'
  echo '  and create a release announcement for the'
  echo '  mailing lists.'
  echo '**************************************************'
fi
