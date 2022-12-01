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
[[ -z $REQUIRED_THRIFT_VERSION ]] && REQUIRED_THRIFT_VERSION='0.17.0'
[[ -z $INCLUDED_MODULES ]] && INCLUDED_MODULES=()
[[ -z $BASE_OUTPUT_PACKAGE ]] && BASE_OUTPUT_PACKAGE='org.apache.accumulo.core'
[[ -z $PACKAGES_TO_GENERATE ]] && PACKAGES_TO_GENERATE=(gc master manager tabletserver securityImpl clientImpl dataImpl trace compaction)
[[ -z $BUILD_DIR ]] && BUILD_DIR='target'
[[ -z $LANGUAGES_TO_GENERATE ]] && LANGUAGES_TO_GENERATE=(java)
[[ -z $FINAL_DIR ]] && FINAL_DIR='src/main'
# ========================================================================================================================

fail() {
  echo "$@"
  exit 1
}

# Test to see if we have thrift installed
if ! thrift -version 2>/dev/null | grep -qF "${REQUIRED_THRIFT_VERSION}"; then
  # Nope: bail
  echo "****************************************************"
  echo "*** thrift is not available"
  echo "***   expecting 'thrift -version' to return ${REQUIRED_THRIFT_VERSION}"
  echo "*** generated code will not be updated"
  fail "****************************************************"
fi

# Include thrift sources from additional modules
THRIFT_ARGS=()
for i in "${INCLUDED_MODULES[@]}"; do
  if [[ $i != '-' ]]; then
    test -d "$i" || fail missing required included module "$i"
    THRIFT_ARGS=("${THRIFT_ARGS[@]}" -I "$i/src/main/thrift")
  fi
done

# Ensure output directories are created
THRIFT_ARGS=("${THRIFT_ARGS[@]}" -o "$BUILD_DIR")
mkdir -p "$BUILD_DIR"
rm -rf "$BUILD_DIR"/gen-java
for f in src/main/thrift/*.thrift; do
  thrift "${THRIFT_ARGS[@]}" --gen java:generated_annotations=suppress "$f" || fail unable to generate java thrift classes
  thrift "${THRIFT_ARGS[@]}" --gen py "$f" || fail unable to generate python thrift classes
  thrift "${THRIFT_ARGS[@]}" --gen rb "$f" || fail unable to generate ruby thrift classes
  thrift "${THRIFT_ARGS[@]}" --gen cpp "$f" || fail unable to generate cpp thrift classes
done

# For all generated thrift code, get rid of all warnings and add the LICENSE header

# add dummy method to suppress "unnecessary suppress warnings" for classes which don't have any unused variables
# this only affects classes, enums aren't affected
#shellcheck disable=SC1004
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
    mapfile -t ALL_FILES_TO_LICENSE < <(find "$BUILD_DIR/gen-$lang" -name "*$file")
    for f in "${ALL_FILES_TO_LICENSE[@]}"; do
      cat - "$f" >"${f}-with-license" <<EOF
${PREFIX}${LINE_NOTATION} Licensed to the Apache Software Foundation (ASF) under one
${LINE_NOTATION} or more contributor license agreements.  See the NOTICE file
${LINE_NOTATION} distributed with this work for additional information
${LINE_NOTATION} regarding copyright ownership.  The ASF licenses this file
${LINE_NOTATION} to you under the Apache License, Version 2.0 (the
${LINE_NOTATION} "License"); you may not use this file except in compliance
${LINE_NOTATION} with the License.  You may obtain a copy of the License at
${LINE_NOTATION}
${LINE_NOTATION}   https://www.apache.org/licenses/LICENSE-2.0
${LINE_NOTATION}
${LINE_NOTATION} Unless required by applicable law or agreed to in writing,
${LINE_NOTATION} software distributed under the License is distributed on an
${LINE_NOTATION} "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
${LINE_NOTATION} KIND, either express or implied.  See the License for the
${LINE_NOTATION} specific language governing permissions and limitations
${LINE_NOTATION} under the License.${SUFFIX}
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
        DDIR="${FINAL_DIR}/thrift-gen-$lang/${BASE_OUTPUT_PACKAGE//.//}/${d//.//}/thrift"
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
      mapfile -t ALL_EXISTING_FILES < <(find "$DDIR" -name "*$file")
      for f in "${ALL_EXISTING_FILES[@]}"; do
        if [[ ! -f "$SDIR/$(basename "$f")-with-license" ]]; then
          set -x
          rm -f "$f"
          { set +x; } 2>/dev/null
        fi
      done
      mapfile -t ALL_LICENSE_FILES_TO_COPY < <(find "$SDIR" -name "*$file")
      for f in "${ALL_LICENSE_FILES_TO_COPY[@]}"; do
        DEST="$DDIR/$(basename "$f")"
        if ! cmp -s "${f}-with-license" "${DEST}"; then
          set -x
          cp -f "${f}-with-license" "${DEST}" || fail unable to copy files to java workspace
          { set +x; } 2>/dev/null
        fi
      done
    done
  done
done
