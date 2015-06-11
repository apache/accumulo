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

loc=$(dirname "$0")
loc=$(cd "$loc/.."; pwd)

cd "$loc"

fail() {
  echo '   ' "$@"
  exit 1
}

run() {
  echo "$@"
  eval "$@"
  if [[ $? != 0 ]] ; then
    fail "$@" fails
  fi
}

runAt() {
  ( cd "$1" ; echo "in $(pwd)"; shift ; run "$@" ) || fail
}

cacheGPG() {
  # make sure gpg agent has key cached
  # TODO prompt for key instead of using default?
  TESTFILE="/tmp/${USER}-gpgTestFile-$(date -u +%s).txt"
  touch "${TESTFILE}" && gpg --sign "${TESTFILE}" && rm -f "${TESTFILE}" "${TESTFILE}.gpg"
}

createEmail() {
  read -p 'Enter the staging repository number: ' stagingrepo
  read -p 'Enter the version to be released (eg. x.y.z): ' tag
  read -p 'Enter the release candidate number (eg. 1, 2, etc.): ' rc

  commit=$(git show $tag -n1 --pretty=raw --no-color | head -1 | awk '{print $2}')
  branch=$tag-rc$rc
  echo
  echo    "IMPORTANT!! IMPORTANT!! IMPORTANT!! IMPORTANT!! IMPORTANT!! IMPORTANT!!"
  echo
  echo    "    Don't forget to push a branch named $branch with"
  echo    "    its head at ${commit:0:7} so people can review!"
  echo
  echo    "    However, do *NOT* push the $tag tag until after the vote"
  echo    "    passes and the tag is re-made with a gpg signature using"
  echo    "    \`git tag -f -m 'Apache Accumulo $tag' -s $tag ${commit:0:7}\`"
  echo
  echo    "IMPORTANT!! IMPORTANT!! IMPORTANT!! IMPORTANT!! IMPORTANT!! IMPORTANT!!"
  echo
  read -p 'Press Enter to generate the [VOTE] email...' rc

  # compute the date with a buffer of 30 minutes
  votedate=$(date -d "+3 days 30 minutes" "+%s")
  # round back to the previous half-hour
  halfhour=$(($votedate - ($votedate % 1800)))
  votedate=$(date -u -d"1970-01-01 $halfhour seconds UTC")
  export TZ="America/New_York"
  edtvotedate=$(date -d"1970-01-01 $halfhour seconds UTC")
  export TZ="America/Los_Angeles"
  pdtvotedate=$(date -d"1970-01-01 $halfhour seconds UTC")

  cat <<EOF
============================================================
Subject: [VOTE] Accumulo $branch
============================================================

Accumulo Developers,

Please consider the following candidate for Accumulo $tag.

Git Commit:
    $commit
Branch:
    $branch

If this vote passes, a gpg-signed tag will be created using:
    git tag -f -m 'Apache Accumulo $tag' -s $tag $commit

Staging repo: https://repository.apache.org/content/repositories/orgapacheaccumulo-$stagingrepo
Source (official release artifact): https://repository.apache.org/content/repositories/orgapacheaccumulo-$stagingrepo/org/apache/accumulo/accumulo/$tag/accumulo-$tag-src.tar.gz
Binary: https://repository.apache.org/content/repositories/orgapacheaccumulo-$stagingrepo/org/apache/accumulo/accumulo/$tag/accumulo-$tag-bin.tar.gz
(Append ".sha1", ".md5", or ".asc" to download the signature/hash for a given artifact.)

All artifacts were built and staged with:
    mvn release:prepare && mvn release:perform

Signing keys are available at https://www.apache.org/dist/accumulo/KEYS
(Expected fingerprint: $(gpg --list-secret-keys --with-colons --with-fingerprint | awk -F: '$1 == "fpr" {print $10}'))

Release notes (in progress) can be found at https://accumulo.apache.org/release_notes/$tag

Please vote one of:
[ ] +1 - I have verified and accept...
[ ] +0 - I have reservations, but not strong enough to vote against...
[ ] -1 - Because..., I do not accept...
... these artifacts as the $tag release of Apache Accumulo.

This vote will end on $votedate
($edtvotedate / $pdtvotedate)

Thanks!

P.S. Hint: download the whole staging repo with
    wget -erobots=off -r -l inf -np -nH \\
    https://repository.apache.org/content/repositories/orgapacheaccumulo-$stagingrepo/
    # note the trailing slash is needed

============================================================
EOF
}

if [[ $1 = '--create-release-candidate' ]]; then
  cacheGPG
  # create a release candidate from a branch
  run mvn clean release:clean release:prepare release:perform
elif [[ $1 = '--test' ]]; then
  cacheGPG
  # build a tag, but with tests
  run mvn clean install \
   -P apache-release,thrift,assemble,docs,accumulo-release
elif [[ $1 = '--create-email' ]]; then
  createEmail
else
  fail "Missing one of: --create-release-candidate, --test, --create-email"
fi

