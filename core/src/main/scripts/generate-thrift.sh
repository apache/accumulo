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

# This script will regenerate the thrift code for Accumulo's RPC mechanisms.

# NOTES:
#   To support this script being called by other modules, only edit the right side.
#   In other scripts, set the variables that diverge from the defaults below, then call this script.
#   PACKAGES_TO_GENERATE should be an array, and each element should be the portion of the dot-separated Java package
#     name following the BASE_OUTPUT_PACKAGE
#   Leave the BUILD_DIR and FINAL_DIR alone for Maven builds.
#   INCLUDED_MODULES should be an array that includes other Maven modules with src/main/thrift directories
#   Use INCLUDED_MODULES=(-) in calling scripts that require no other modules
# ========================================================================================================================
[[ -z $REQUIRED_THRIFT_VERSION ]] && REQUIRED_THRIFT_VERSION='0.10.0'
[[ -z $INCLUDED_MODULES ]]        && INCLUDED_MODULES=(../server/tracer)
[[ -z $BASE_OUTPUT_PACKAGE ]]     && BASE_OUTPUT_PACKAGE='org.apache.accumulo.core'
[[ -z $PACKAGES_TO_GENERATE ]]    && PACKAGES_TO_GENERATE=(gc master tabletserver security client.impl data replication trace)
[[ -z $BUILD_DIR ]]               && BUILD_DIR='target'
[[ -z $LANGUAGES_TO_GENERATE ]]   && LANGUAGES_TO_GENERATE=(java)
[[ -z $FINAL_DIR ]]               && FINAL_DIR='src/main'
# ========================================================================================================================

fail() {
  echo "$@"
  exit 1
}

# Test to see if we have thrift installed
VERSION=$(thrift -version 2>/dev/null | grep -F "${REQUIRED_THRIFT_VERSION}" |  wc -l | sed -e 's/^ *//' -e 's/ *$//')
if [[ "${VERSION}" != '1' ]] ; then
  # Nope: bail
  echo "****************************************************"
  echo "*** thrift is not available"
  echo "***   expecting 'thrift -version' to return ${REQUIRED_THRIFT_VERSION}"
  echo "*** generated code will not be updated"
  fail "****************************************************"
fi

# Include thrift sources from additional modules
THRIFT_ARGS=''
for i in "${INCLUDED_MODULES[@]}"; do
  if [ ${i} != '-' ]; then
    test -d ${i} || fail missing required included module ${i}
    THRIFT_ARGS="${THRIFT_ARGS} -I ${i}/src/main/thrift"
  fi
done

# Ensure output directories are created
THRIFT_ARGS="${THRIFT_ARGS} -o $BUILD_DIR"
mkdir -p $BUILD_DIR
rm -rf $BUILD_DIR/gen-java
for f in src/main/thrift/*.thrift; do
  thrift ${THRIFT_ARGS} --gen java:generated_annotations=undated "$f" || fail unable to generate java thrift classes
  thrift ${THRIFT_ARGS} --gen py "$f" || fail unable to generate python thrift classes
  thrift ${THRIFT_ARGS} --gen rb "$f" || fail unable to generate ruby thrift classes
  thrift ${THRIFT_ARGS} --gen cpp "$f" || fail unable to generate cpp thrift classes
done

# For all generated thrift code, get rid of all warnings and add the LICENSE header

# workaround for THRIFT-4062; should be fixed in newer thrift versions
find $BUILD_DIR/gen-java -name '*.java' -exec sed -i -e 's/\(org[.]apache[.]\)thrift\([.]TServiceClient\) /\1accumulo.core.rpc\2Wrapper /' {} +
# upstream stopped doing import statements for classes, but overlooked enums; delete unused imports
find $BUILD_DIR/gen-java -name '*.java' -exec grep -Zl '^public enum ' {} + | xargs -0 sed -i -e '/^import .*$/d'
# add dummy method to suppress "unnecessary suppress warnings" for classes which don't have any unused variables
# this only affects classes, enums aren't affected
find $BUILD_DIR/gen-java -name '*.java' -exec grep -Zl '^public class ' {} + | xargs -0 sed -i -e 's/^[}]$/  private static void unusedMethod() {}\
}/'

for lang in "${LANGUAGES_TO_GENERATE[@]}"; do
  case $lang in
    cpp)
      PREFIX="/*
"
      LINE_NOTATION=" *"
      SUFFIX="
 */"
      FILE_SUFFIX=(.h .cpp)
      ;;
    java)
      PREFIX="/*
"
      LINE_NOTATION=" *"
      SUFFIX="
 */"
      FILE_SUFFIX=(.java)
      ;;
    rb)
      PREFIX=""
      LINE_NOTATION="#"
      SUFFIX=""
      FILE_SUFFIX=(.rb)
      ;;
    py)
      PREFIX=""
      LINE_NOTATION="#"
      SUFFIX=""
      FILE_SUFFIX=(.py -remote)
      ;;
    *)
      continue
      ;;
  esac

  for file in "${FILE_SUFFIX[@]}"; do
    for f in $(find $BUILD_DIR/gen-$lang -name "*$file"); do
      cat - "$f" > "${f}-with-license" <<EOF
${PREFIX}${LINE_NOTATION} Licensed to the Apache Software Foundation (ASF) under one or more
${LINE_NOTATION} contributor license agreements.  See the NOTICE file distributed with
${LINE_NOTATION} this work for additional information regarding copyright ownership.
${LINE_NOTATION} The ASF licenses this file to You under the Apache License, Version 2.0
${LINE_NOTATION} (the "License"); you may not use this file except in compliance with
${LINE_NOTATION} the License.  You may obtain a copy of the License at
${LINE_NOTATION}
${LINE_NOTATION}     http://www.apache.org/licenses/LICENSE-2.0
${LINE_NOTATION}
${LINE_NOTATION} Unless required by applicable law or agreed to in writing, software
${LINE_NOTATION} distributed under the License is distributed on an "AS IS" BASIS,
${LINE_NOTATION} WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
${LINE_NOTATION} See the License for the specific language governing permissions and
${LINE_NOTATION} limitations under the License.${SUFFIX}
EOF
    done
  done
done

# For every generated java file, compare it with the version-controlled one, and copy the ones that have changed into place
for d in "${PACKAGES_TO_GENERATE[@]}"; do
  for lang in "${LANGUAGES_TO_GENERATE[@]}"; do
    case "$lang" in
      cpp)
        SDIR="${BUILD_DIR}/gen-$lang/"
        DDIR="${FINAL_DIR}/cpp/"
        FILE_SUFFIX=(.h .cpp)
        ;;
      java)
        SDIR="${BUILD_DIR}/gen-$lang/${BASE_OUTPUT_PACKAGE//.//}/${d//.//}/thrift"
        DDIR="${FINAL_DIR}/java/${BASE_OUTPUT_PACKAGE//.//}/${d//.//}/thrift"
        FILE_SUFFIX=(.java)
        ;;
      rb)
        SDIR="${BUILD_DIR}/gen-$lang/"
        DDIR="${FINAL_DIR}/ruby/"
        FILE_SUFFIX=(.rb)
        ;;
      py)
        SDIR="${BUILD_DIR}/gen-$lang/accumulo"
        DDIR="${FINAL_DIR}/python/"
        FILE_SUFFIX=(.py -remote)
        ;;
      *)
        continue
        ;;
    esac
    mkdir -p "$DDIR"
    for file in "${FILE_SUFFIX[@]}"; do
      for f in $(find $SDIR -name *$file); do
        DEST="$DDIR/$(basename $f)"
        if ! cmp -s "${f}-with-license" "${DEST}" ; then
          echo cp -f "${f}-with-license" "${DEST}"
          cp -f "${f}-with-license" "${DEST}" || fail unable to copy files to java workspace
        fi
      done
    done
  done
done
