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

function addAccumuloAPI() {
  local srcDir="$1" api apiPath
  # Validate each source directory before populating JShell-Init file
  if [[ ! -d $srcDir ]]; then
    echo "$srcDir is not a valid directory. Please make sure it exists."
    exit 1
  fi

  # Extract API info from provided source directory
  mapfile -t api < <(find "$srcDir" -type f -name '*.java' -print0 | xargs -0 -n1 dirname | sort -u)

  # Load in API and format source directory into Java import statements
  for apiPath in "${api[@]}"; do
    echo "import ${apiPath##*/java/}.*;" | tr / .
  done
  echo
}

function addClientBuild() {
  cat <<EOF
  URL clientPropUrl =
    AccumuloClient.class.getClassLoader().getResource("accumulo-client.properties");
  AccumuloClient client = null;

  // Does Accumulo properties exists?
  if (clientPropUrl != null) {

    // Build Accumulo Client
    client = Accumulo.newClient().from(clientPropUrl).build();
    System.out.println("Use 'client' to interact with Accumulo\n");

  } else
      System.out.println("'accumulo-client.properties' was not found on the classpath\n");
EOF
}

function main() {
  local SOURCE bin scriptPath mainBase corePath
  # Establish Accumulo's main base directory
  SOURCE="${BASH_SOURCE[0]}"
  while [[ -L ${SOURCE} ]]; do
    bin="$(cd -P "$(dirname "${SOURCE}")" && pwd)"
    SOURCE="$(readlink "${SOURCE}")"
    [[ ${SOURCE} != /* ]] && SOURCE="${bin}/${SOURCE}"
  done

  # Establish file and folder paths for JShell config
  scriptPath="$(cd -P "$(dirname "${SOURCE}")" && pwd)"
  mainBase="$(cd -P "${scriptPath}"/../../../.. && pwd)"
  corePath="$mainBase/core/src/main/java/org/apache/accumulo/core"

  # Create new jshell-init file
  mkdir -p "$mainBase/assemble/target"
  echo 'Generating JShell-Init file'
  {
    echo '// Accumulo Client API'
    addAccumuloAPI "$corePath/client"
    echo '// Accumulo Data API'
    addAccumuloAPI "$corePath/data"
    echo '// Accumulo Security API'
    addAccumuloAPI "$corePath/security"
    echo '// Accumulo MiniCluster API'
    addAccumuloAPI "$mainBase/minicluster/src/main/java/org/apache/accumulo/minicluster"
    echo '// Accumulo Hadoop API'
    addAccumuloAPI "$mainBase/hadoop-mapreduce/src/main/java/org/apache/accumulo/hadoop/mapreduce"
    echo '// Essential Hadoop API'
    echo 'import org.apache.hadoop.io.Text;'
    echo
    echo '// Initialization Code'
    echo 'System.out.println("Preparing JShell for Apache Accumulo");'
    echo 'System.out.println();'
    echo
    echo '// Accumulo Client Build'
    addClientBuild
  } >"$mainBase/assemble/target/jshell-init.jsh"
}

main "$@"
