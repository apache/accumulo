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
  export srcDir="$1"
  export jPath="$2"
  export client="accumulo/core/client"
  export data="accumulo/core/data"
  export security="accumulo/core/security"
  export mini="accumulo/minicluster"
  export hadoop="accumulo/hadoop"
    
  # Does an auto-generated JShell config file exists?
  if [[ ! -e "$jPath" ]]; then
    echo "Cannot add APIs in $jPath"
    echo "Please ensure jshell-init.jsh exists"
    exit 1
  fi
  
  # Is the source directory valid?
  if [[ ! -d "$srcDir" ]]; then
    echo "$srcDir is not a valid directory. Please make sure it exists."
    rm "$jPath"
    exit 1
  fi
  
  # Does a valid JShell path and source directory exists?
  if [[ -e "$jPath" ]] && [[ -d "$srcDir" ]]; then
    # Add API category designator in jshell-init.jsh 
    case "$srcDir" in
        *"$client"*) echo "// Accumulo Client API" >> "$jPath";;
        *"$data"*) echo "// Accumulo Data API" >> "$jPath";;
        *"$security"*) echo "// Accumulo Security API" >> "$jPath";;
        *"$mini"*) echo "// Accumulo Minicluster API" >> "$jPath";;
        *"$hadoop"*) echo "// Accumulo Hadoop API" >> "$jPath";;
        *) echo "// Other API" >> "$jPath";;
    esac
   
    # Extract API info from provided source directory
    mapfile -t api < <(find "$srcDir" -type f -name '*.java'| 
                       xargs -n1 dirname| sort -u)
   
    # Load in API and format source directory into Java import statements
    for apiPath in "${api[@]}"; do
       printf "%s\n" "import ${apiPath##*/java/}.*" >> "$jPath"
    done
    sed -i '/^ *import / s#/#.#g' "$jPath"
    echo " " >> "$jPath"
  fi
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
  export scriptPath="$( cd -P "$( dirname "${SOURCE}" )" && pwd )"; 
  export mainBase=$( cd -P "${scriptPath}"/../../../.. && pwd );
  export jPath="$mainBase/assemble/target/jshell-init.jsh"
  export corePath="core/src/main/java/org/apache/accumulo/core"
  export miniPath="minicluster/src/main/java/org/apache/accumulo"
  export hadoopPath="hadoop-mapreduce/src/main/java/org/apache/accumulo"
    
  # Create path to Accumulo Public API Source Directories
  export CLIENT="$mainBase/$corePath/client"
  export DATA="$mainBase/$corePath/data"
  export SECURITY="$mainBase/$corePath/security"
  export MINI="$mainBase/$miniPath/minicluster"
  export HADOOP="$mainBase/$hadoopPath/hadoop/mapreduce" 
   
  # Does an auto-generated JShell config file exists?
  if [[ -e "$jPath" ]]; then
     rm "$jPath"
  fi
    
  # Create new jshell-init file and load in license header   
  touch "$jPath"
 
  # Create and add Accumulo APIs into API storage 
  apiStorage=("$CLIENT" "$DATA" "$SECURITY" "$MINI" "$HADOOP")
   
  # Traverse through each source directory and load in Accumulo APIs
  for srcDir in "${apiStorage[@]}"; do
    addAccumuloAPI "$srcDir" "$jPath"
  done
  exit 0
}
main "$@"
