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

function addAccumuloAPI(){
  local srcDir="$1"
  # Extract API info from provided source directory
  mapfile -t api < <(find "$srcDir" -type f -name '*.java' -print0|
                     xargs -0 -n1 dirname| sort -u)

  # Load in API and format source directory into Java import statements
  for apiPath in "${api[@]}"; do
     echo "import ${apiPath##*/java/}.*" | tr / .
  done
  echo
}

function main(){
  # Establish Accumulo's main base directory
  SOURCE="${BASH_SOURCE[0]}"
  while [[ -h "${SOURCE}" ]]; do
    bin="$( cd -P "$( dirname "${SOURCE}" )" && pwd )"
    SOURCE="$(readlink "${SOURCE}")"
    [[ "${SOURCE}" != /* ]] && SOURCE="${bin}/${SOURCE}"
  done

  # Establish file and folder paths for JShell config
  local scriptPath
  scriptPath="$( cd -P "$( dirname "${SOURCE}" )" && pwd )"
  local mainBase
  mainBase="$( cd -P "${scriptPath}"/../../../.. && pwd )"
  local jPath="$mainBase/assemble/target/jshell-init.jsh"
  local corePath="core/src/main/java/org/apache/accumulo/core"
  local miniPath="minicluster/src/main/java/org/apache/accumulo"
  local hadoopPath="hadoop-mapreduce/src/main/java/org/apache/accumulo"

  # Create path to Accumulo Public API Source Directories
  local CLIENT="$mainBase/$corePath/client"
  local DATA="$mainBase/$corePath/data"
  local SECURITY="$mainBase/$corePath/security"
  local MINI="$mainBase/$miniPath/minicluster"
  local HADOOP="$mainBase/$hadoopPath/hadoop/mapreduce"

  # Create new jshell-init file
  mkdir -p "$mainBase/assemble/target"
  :> "$jPath"

  # Create and add Accumulo APIs into API storage
  local apiStorage=("$CLIENT" "$DATA" "$SECURITY" "$MINI" "$HADOOP")
  local srcDir

  # Validate each source directory before populating JShell-Init file
  for srcDir in "${apiStorage[@]}"; do
    if [[ ! -d "$srcDir" ]]; then
      echo "Could not auto-generate jshell-init.jsh"
      echo "$srcDir is not a valid directory. Please make sure it exists."
      rm "$jPath"
      exit 1
    fi
  done
  echo "Generating JShell-Init file"
  {
    echo "System.out.println(\"Preparing JShell for Apache Accumulo\")"
    echo "// Accumulo Client API"
    addAccumuloAPI "${apiStorage[0]}"
    echo "// Accumulo Data API"
    addAccumuloAPI "${apiStorage[1]}"
    echo "// Accumulo Security API"
    addAccumuloAPI "${apiStorage[2]}"
    echo "// Accumulo MiniCluster API"
    addAccumuloAPI "${apiStorage[3]}"
    echo "// Accumulo Hadoop API"
    addAccumuloAPI "${apiStorage[4]}"
  } > "$jPath"
}
main "$@"
