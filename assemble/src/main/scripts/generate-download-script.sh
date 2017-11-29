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

# This script will generate a DEPENDENCIES listing of packaged dependencies

in=target/dependencies.raw.txt
out=target/download-dependencies

cat >"$out" <<'EOF'
#! /usr/bin/env bash
# This script downloads the following jars, identified by their maven
# coordinates, using the maven-dependency-plugin.
#
# DISCLAIMER: This is only one possible way to download a set of dependencies
# for your class path. This is not guaranteed to download the versions of
# dependencies you require for your particular installation of Accumulo. It is recommended
# that you consider your class path carefully, and determine which dependencies
# are suitable for your needs.

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

cat >"$DIR/VERSIONS" <<'HEAD'
This file is GENERATED and lists the versions of the dependencies downloaded by
the download-dependencies script when it was last run, in the form:

  <groupId>:<artifactId>:<version>

HEAD

downloadDependency () {
  echo "Downloading to $DIR/$1 ..."
  echo "  $1" >>"$DIR/VERSIONS"
  mvn org.apache.maven.plugins:maven-dependency-plugin:2.10:copy -Dartifact="$1" -Dmdep.stripVersion=true -DoutputDirectory="$DIR"
}

EOF

for x in $(grep '^   [a-z]' "$in"); do
  IFS=:
  read -ra JAR <<< "$x"
  echo "downloadDependency ${JAR[0]}:${JAR[1]}:${JAR[3]}:${JAR[2]}" >>"$out"
done
