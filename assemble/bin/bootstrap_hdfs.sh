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

# Start: Resolve Script Directory
SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
   bin="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
   SOURCE="$(readlink "$SOURCE")"
   [[ $SOURCE != /* ]] && SOURCE="$bin/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
bin="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
# Stop: Resolve Script Directory

. "$bin"/config.sh

#
# Find the system context directory in HDFS
#
SYSTEM_CONTEXT_HDFS_DIR=$(grep -A1 "general.vfs.classpaths" "$ACCUMULO_CONF_DIR/accumulo-site.xml" | tail -1 | perl -pe 's/\s+<value>//; s/<\/value>//; s|[^/]+$||; print $ARGV[1]')

if [ -z "$SYSTEM_CONTEXT_HDFS_DIR" ]
then
   echo "Your accumulo-site.xml file is not set up for the HDFS Classloader. Please add the following to your accumulo-site.xml file:"
   echo ""
   echo "<property>"
   echo "   <name>general.vfs.classpaths</name>"
   echo "   <value>hdfs://host:port/dir</value>"
   echo "   <description>location of the jars for the default (system) context</description>"
   echo "</property>"
   exit 1
fi

#
# Create the system context directy in HDFS if it does not exist
#
"$HADOOP_PREFIX/bin/hadoop" fs -ls "$SYSTEM_CONTEXT_HDFS_DIR"  > /dev/null
if [[ $? != 0 ]]; then
   "$HADOOP_PREFIX/bin/hadoop" fs -mkdir "$SYSTEM_CONTEXT_HDFS_DIR"  > /dev/null
fi

#
# Replicate to all slaves to avoid network contention on startup
#
SLAVES=$ACCUMULO_CONF_DIR/slaves
NUM_SLAVES=$(egrep -v '(^#|^\s*$)' "$SLAVES" | wc -l)

#let each datanode service around 50 clients
REP=$(( NUM_SLAVES / 50 ))
(( REP < 3 )) && REP=3

#
# Copy all jars in lib to the system context directory
#
"$HADOOP_PREFIX/bin/hadoop" fs -moveFromLocal "$ACCUMULO_HOME"/lib/*.jar "$SYSTEM_CONTEXT_HDFS_DIR"  > /dev/null
"$HADOOP_PREFIX/bin/hadoop" fs -setrep -R $REP "$SYSTEM_CONTEXT_HDFS_DIR"  > /dev/null

#
# We need two of the jars in lib, copy them back out and remove them from the system context dir
#
"$HADOOP_PREFIX/bin/hadoop" fs -copyToLocal "$SYSTEM_CONTEXT_HDFS_DIR/commons-vfs2.jar" "$ACCUMULO_HOME/lib/."  > /dev/null
"$HADOOP_PREFIX/bin/hadoop" fs -rmr "$SYSTEM_CONTEXT_HDFS_DIR/commons-vfs2.jar"  > /dev/null
"$HADOOP_PREFIX/bin/hadoop" fs -copyToLocal "$SYSTEM_CONTEXT_HDFS_DIR/accumulo-start.jar" "$ACCUMULO_HOME/lib/."  > /dev/null
"$HADOOP_PREFIX/bin/hadoop" fs -rmr "$SYSTEM_CONTEXT_HDFS_DIR/accumulo-start.jar"  > /dev/null
for f in $(grep -v '^#' "$ACCUMULO_CONF_DIR/slaves")
do
  rsync -ra --delete "$ACCUMULO_HOME" $(dirname "$ACCUMULO_HOME")
done
