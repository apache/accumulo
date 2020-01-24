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
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

cd "$(dirname "$0")/.." || exit 1
scriptname=$(basename "$0")
export tlpName=accumulo
export projName="$tlpName"
export projNameLong="Apache ${projName^}"
export stagingRepoPrefix="https://repository.apache.org/content/repositories/orgapache$tlpName"
export srcQualifier="src"
export relTestingUrl="https://$tlpName.apache.org/contributor/verifying-release"
export tagPrefix="rel/"

# check for gpg2
hash gpg2 2>/dev/null && gpgCommand=gpg2 || gpgCommand=gpg

# check if running in a color terminal
terminalSupportsColor() {
  local c; c=$(tput colors 2>/dev/null) || c=-1
  [[ -t 1 ]] && [[ $c -ge 8 ]]
}
terminalSupportsColor && doColor=1 || doColor=0

color() { local c; c=$1; shift; [[ $doColor -eq 1 ]] && echo -e "\\e[0;${c}m${*}\\e[0m" || echo "$@"; }
red() { color 31 "$@"; }
green() { color 32 "$@"; }
yellow() { color 33 "$@"; }

fail() { echo -e ' ' "$@"; exit 1; }
runLog() { local o; o=$1 && shift && echo "$(green Running) $(yellow "$@" '>>' "$o")" && echo Running "$@" >> "$o" && eval "$@" >> "$o"; }
run() { echo "$(green Running) $(yellow "$@")" && eval "$@"; }
runOrFail() { run "$@" || fail "$(yellow "$@")" "$(red failed)"; }

currentBranch() { local b; b=$(git symbolic-ref -q HEAD) && echo "${b##refs/heads/}"; }

cacheGPG() {
  # make sure gpg agent has key cached
  # first clear cache, to reset timeouts (best attempt)
  { hash gpg-connect-agent && gpg-connect-agent reloadagent /bye; } &>/dev/null
  # TODO prompt for key instead of using default?
  local TESTFILE; TESTFILE=$(mktemp --tmpdir "${USER}-gpgTestFile-XXXXXXXX.txt")
  [[ -r $TESTFILE ]] && "$gpgCommand" --sign "${TESTFILE}" && rm -f "${TESTFILE}" "${TESTFILE}.gpg"
}

prompter() {
  # $1 description; $2 pattern to validate against
  local x
  read -r -p "Enter the $1: " x
  until eval "[[ \$x =~ ^$2\$ ]]"; do
    echo "  $(red "$x") is not a proper $1" 1>&2
    read -r -p "Enter the $1: " x
  done
  echo "$x"
}

pretty() { local f; f=$1; shift; git log "--pretty=tformat:$f" "$@"; }
gitCommits() { pretty %H "$@"; }
gitCommit()  { gitCommits -n1 "$@"; }
gitSubject() { pretty %s "$@"; }

createEmail() {
  # $1 version (optional); $2 rc sequence num (optional); $3 staging repo num (optional)
  local ver; [[ -n "$1" ]] && ver=$1 || ver=$(prompter 'version to be released (eg. x.y.z)' '[0-9]+[.][0-9]+[.][0-9]+')
  local rc; [[ -n "$2" ]] && rc=$2 || rc=$(prompter 'release candidate sequence number (eg. 1, 2, etc.)' '[0-9]+')
  local stagingrepo; [[ -n "$3" ]] && stagingrepo=$3 || stagingrepo=$(prompter 'staging repository number from https://repository.apache.org/#stagingRepositories' '[0-9]+')
  local srcSha; [[ -n "$4" ]] && srcSha=$4 || srcSha=$(prompter 'SHA512 for source tarball' '[0-9a-f]{128}')
  local binSha; [[ -n "$5" ]] && binSha=$5 || binSha=$(prompter 'SHA512 for binary tarball' '[0-9a-f]{128}')

  local branch; branch=$ver-rc$rc
  local commit; commit=$(gitCommit "$branch") || exit 1
  local tag; tag=$tagPrefix$ver
  echo
  yellow  "IMPORTANT!! IMPORTANT!! IMPORTANT!! IMPORTANT!! IMPORTANT!! IMPORTANT!!"
  echo
  echo    "    Don't forget to push a branch named $(green "$branch") with"
  echo    "    its head at $(green "${commit:0:7}") so others can review using:"
  echo    "      $(green "git push origin ${commit:0:7}:refs/heads/$branch")"
  echo
  echo    "    Remember, $(red DO NOT PUSH) the $(red "$tag") tag until after the vote"
  echo    "    passes and the tag is re-made with a gpg signature using:"
  echo    "      $(red "git tag -f -m '$projNameLong $ver' -s $tag ${commit:0:7}")"
  echo
  yellow  "IMPORTANT!! IMPORTANT!! IMPORTANT!! IMPORTANT!! IMPORTANT!! IMPORTANT!!"
  echo
  read -r -s -p 'Press Enter to generate the [VOTE] email...'
  echo 1>&2

  # compute the date with a buffer of 30 minutes
  local votedate; votedate=$(date -d "+3 days 30 minutes" "+%s")
  # round back to the previous half-hour
  local halfhour; halfhour=$((votedate - (votedate % 1800)))
  votedate=$(date -u -d"1970-01-01 $halfhour seconds UTC")
  export TZ="America/New_York"
  local edtvotedate; edtvotedate=$(date -d"1970-01-01 $halfhour seconds UTC")
  export TZ="America/Los_Angeles"
  local pdtvotedate; pdtvotedate=$(date -d"1970-01-01 $halfhour seconds UTC")

  local fingerprint; fingerprint=$("$gpgCommand" --list-secret-keys --with-colons --with-fingerprint 2>/dev/null | awk -F: '$1 == "fpr" {print $10}')
  [[ -z $fingerprint ]] && fingerprint="UNSPECIFIED"

  cat <<EOF
$(yellow '============================================================')
Subject: $(green [VOTE] "$projNameLong $branch")
$(yellow '============================================================')
${tlpName^} Developers,

Please consider the following candidate for $projNameLong $(green "$ver").

Git Commit:
    $(green "$commit")
Branch:
    $(green "$branch")

If this vote passes, a gpg-signed tag will be created using:
    $(green "git tag -f -m '$projNameLong $ver' -s $tag") \\
    $(green "$commit")

Staging repo: $(green "$stagingRepoPrefix-$stagingrepo")
Source (official release artifact): $(green "$stagingRepoPrefix-$stagingrepo/org/apache/$tlpName/$projName/$ver/$projName-$ver-$srcQualifier.tar.gz")
Binary: $(green "$stagingRepoPrefix-$stagingrepo/org/apache/$tlpName/$projName/$ver/$projName-$ver-bin.tar.gz")

Append ".asc" to download the cryptographic signature for a given artifact.
(You can also append ".sha1" or ".md5" instead in order to verify the checksums
generated by Maven to verify the integrity of the Nexus repository staging area.)

Signing keys are available at https://www.apache.org/dist/$tlpName/KEYS
(Expected fingerprint: $(green "$fingerprint"))

In addition to the tarballs and their signatures, the following checksum
files will be added to the dist/release SVN area after release:
$(yellow "$projName-$ver-$srcQualifier.tar.gz.sha512") will contain:
SHA512 ($(green "$projName-$ver-$srcQualifier.tar.gz")) = $(yellow "$srcSha")
$(yellow "$projName-$ver-bin.tar.gz.sha512") will contain:
SHA512 ($(green "$projName-$ver-bin.tar.gz")) = $(yellow "$binSha")

Release notes (in progress) can be found at: $(green "https://$tlpName.apache.org/release/$projName-$ver/")

Release testing instructions: $relTestingUrl

Please vote one of:
[ ] +1 - I have verified and accept...
[ ] +0 - I have reservations, but not strong enough to vote against...
[ ] -1 - Because..., I do not accept...
... these artifacts as the $(green "$ver") release of $projNameLong.

This vote will remain open until at least $(green "$votedate").
($(green "$edtvotedate") / $(green "$pdtvotedate"))
Voting can continue after this deadline until the release manager
sends an email ending the vote.

Thanks!

P.S. Hint: download the whole staging repo with
    wget -erobots=off -r -l inf -np -nH \\
    $(green "$stagingRepoPrefix-$stagingrepo/")
    # note the trailing slash is needed
$(yellow '============================================================')
EOF
}

cleanUpAndFail() {
  # $1 command; $2 log; $3 original branch; $4 next branch
  echo "  Failure in $(red "$1")!"
  echo "  Check output in $(yellow "$2")"
  echo "  Initiating clean up steps..."

  run git checkout "$3"

  # pre-populate branches with expected next branch; de-duplicate later
  local branches; branches=("$4")
  local tags; tags=()
  local x; local y
  for x in $(gitCommits "${cBranch}..${nBranch}"); do
    for y in $(git branch --contains "$x" | cut -c3-); do
      branches=("${branches[@]}" "$y")
    done
    for y in $(git tag --contains "$x"); do
      tags=("${tags[@]}" "$y")
    done
  done

  # de-duplicate branches
  local a
  local tmpArray; tmpArray=("${branches[@]}")
  IFS=$'\n' read -d '' -r -a branches < <(printf '%s\n' "${tmpArray[@]}" | sort -u)
  for x in "${branches[@]}"; do
    echo "Do you wish to clean up (delete) the branch $(yellow "$x")?"
    a=$(prompter "letter 'y' or 'n'" '[yn]')
    [[ $a == 'y' ]] && git branch -D "$x"
  done
  for x in "${tags[@]}"; do
    echo "Do you wish to clean up (delete) the tag $(yellow "$x")?"
    a=$(prompter "letter 'y' or 'n'" '[yn]')
    [[ $a == 'y' ]] && git tag -d "$x"
  done
  exit 1
}

createReleaseCandidate() {
  yellow  "WARNING!! WARNING!! WARNING!! WARNING!! WARNING!! WARNING!!"
  echo
  echo    "  This will modify your local git repository by creating"
  echo    "  branches and tags. Afterwards, you may need to perform"
  echo    "  some manual steps to complete the release or to rollback"
  echo    "  in the case of failure."
  echo
  yellow  "WARNING!! WARNING!! WARNING!! WARNING!! WARNING!! WARNING!!"
  echo

  local extraReleaseArgs; extraReleaseArgs=("$@")
  if [[ ${#extraReleaseArgs[@]} -ne 0 ]]; then
    red "CAUTION!! Extra release args may create a non-standard release!!"
    red "You added '${extraReleaseArgs[*]}'"
  fi
  [[ ${#extraReleaseArgs[@]} -eq 0 ]] && [[ $gpgCommand != 'gpg' ]] && extraReleaseArgs=("-Dgpg.executable=$gpgCommand")
  local extraReleaseArgsFlat; extraReleaseArgsFlat="-DextraReleaseArguments='${extraReleaseArgs[*]}'"

  local ver
  ver=$(xmllint --shell pom.xml <<<'xpath /*[local-name()="project"]/*[local-name()="version"]/text()' | grep content= | cut -f2 -d=)
  ver=${ver%%-SNAPSHOT}
  echo "Building release candidate for version: $(green "$ver")"
  local tag; tag=$tagPrefix$ver

  local cBranch; cBranch=$(currentBranch) || fail "$(red Failure)" to get current branch from git
  local rc; rc=$(prompter 'release candidate sequence number (eg. 1, 2, etc.)' '[0-9]+')
  local tmpNextVer; tmpNextVer="${ver%.*}.$((${ver##*.}+1))"
  local nextVer; nextVer=$(prompter "next snapshot version to be released [$tmpNextVer]" '([0-9]+[.][0-9]+[.][0-9]+)?')
  [[ -n $nextVer ]] || nextVer=$tmpNextVer
  local rcBranch; rcBranch=$ver-rc$rc
  local nBranch; nBranch=$rcBranch-next

  cacheGPG || fail "Unable to cache GPG credentials into gpg-agent"

  # create working branch
  {
    run git branch "$nBranch" "$cBranch" && run git checkout "$nBranch"
  } || fail "Unable to create working branch $(red "$nBranch") from $(red "$cBranch")!"

  # create a release candidate from a branch
  local oFile; oFile=$(mktemp --tmpdir "$projName-build-$rcBranch-XXXXXXXX.log")
  {
    [[ -w $oFile ]] && runLog "$oFile" mvn clean release:clean
  } || cleanUpAndFail 'mvn clean release:clean' "$oFile" "$cBranch" "$nBranch"
  runLog "$oFile" mvn -B release:prepare -DdevelopmentVersion="${nextVer}-SNAPSHOT" "${extraReleaseArgsFlat}" || \
    cleanUpAndFail "mvn -B release:prepare -DdevelopmentVersion=${nextVer}-SNAPSHOT ${extraReleaseArgsFlat}" "$oFile" "$cBranch" "$nBranch"
  runLog "$oFile" mvn release:perform "${extraReleaseArgsFlat}" || \
    cleanUpAndFail "mvn release:perform ${extraReleaseArgsFlat}" "$oFile" "$cBranch" "$nBranch"

  # switch back to original branch
  run git checkout "${cBranch}"

  # verify the next branch contains both expected log messages and no more
  {
    [[ $(gitCommits "${cBranch}..${nBranch}" | wc -l) -eq 2 ]] && \
      [[ $(gitCommit  "${nBranch}~2") ==  $(gitCommit "${cBranch}") ]] && \
      [[ $(gitSubject "${nBranch}")   =~ ^\[maven-release-plugin\]\ prepare\ for\ next ]] && \
      [[ $(gitSubject "${nBranch}~1") =~ ^\[maven-release-plugin\]\ prepare\ release\ rel[/] ]]
  } || cleanUpAndFail "verifying that $nBranch contains only logs from release plugin"

  # verify the tag is one behind $nBranch and one ahead of $cBranch
  [[ $(gitCommit "${nBranch}~1") == $(gitCommit "refs/tags/$tag") ]] || \
    cleanUpAndFail "verifying that ${nBranch}~1 == refs/tags/$tag"

  # remove tag which was created
  run git tag -d "$tag" || \
    cleanUpAndFail "removing unused git tag $tag"

  # create release candidate branch to vote on
  run git branch "$rcBranch" "${nBranch}~1" || \
    cleanUpAndFail "creating branch $rcBranch"

  # push branches (ask first)
  local origin; origin=$(git remote -v | grep ^origin | grep push | awk '{print $2}')
  echo "Do you wish to push the following branches to origin ($(green "$origin"))?"
  echo "  $(yellow "$rcBranch")      (for others to examine for the vote)"
  echo "  $(yellow "$nBranch") (for merging into $cBranch if vote passes)"
  local a; a=$(prompter "letter 'y' or 'n'" '[yn]')
  {
    [[ $a == 'y' ]] && \
      run git push -u origin "refs/heads/$nBranch" "refs/heads/$rcBranch"
  } || red "Did not push branches; you'll need to perform this step manually."

  local numSrc; numSrc=$(find target/checkout/ -type f -name "$projName-$ver-source-release.tar.gz" | wc -l)
  local numBin; numBin=$(find target/checkout/ -type f -name "$projName-$ver-bin.tar.gz" | wc -l)
  shopt -s globstar
  local srcSha; srcSha=""
  local binSha; binSha=""
  [[ $numSrc = "1" ]] && srcSha=$(sha512sum target/checkout/**/"$projName-$ver-source-release.tar.gz" | cut -f1 -d" ")
  [[ $numBin = "1" ]] && binSha=$(sha512sum target/checkout/**/"$projName-$ver-bin.tar.gz" | cut -f1 -d" ")

  # continue to creating email notification
  echo "$(red Running)" "$(yellow "$scriptname" --create-email "$ver" "$rc")"
  createEmail "$ver" "$rc" "" "$srcSha" "$binSha"
}

if [[ $1 == '--create-release-candidate' ]]; then
  shift
  createReleaseCandidate "$@"
elif [[ $1 == '--test' ]]; then
  cacheGPG
  # build a tag, but with tests
  runOrFail mvn clean install -P apache-release,accumulo-release,thrift
elif [[ $1 == '--create-email' ]]; then
  shift
  createEmail "$@"
else
  fail "Missing one of: $(red --create-release-candidate), $(red --test), $(red --create-email)"
fi

