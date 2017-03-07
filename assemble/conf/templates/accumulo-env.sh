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

## Before accumulo-env.sh is loaded, these environment variables are set and can be used in this file:

# cmd - Command that is being called such as tserver, master, etc.
# basedir - Root of Accumulo installation
# bin - Directory containing Accumulo scripts
# conf - Directory containing Accumulo configuration
# lib - Directory containing Accumulo libraries

############################
# Variables that must be set
############################

## Accumulo logs directory. Referenced by logger config.
export ACCUMULO_LOG_DIR="${ACCUMULO_LOG_DIR:-${basedir}/logs}"
## Hadoop installation
export HADOOP_PREFIX="${HADOOP_PREFIX:-/path/to/hadoop}"
## Hadoop configuration
export HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-${HADOOP_PREFIX}/etc/hadoop}"
## Zookeeper installation
export ZOOKEEPER_HOME="${ZOOKEEPER_HOME:-/path/to/zookeeper}"

##################################################################
# Build JAVA_OPTS variable. Defaults below work but can be edited.
##################################################################

## JVM options set for all processes. Extra options can be passed in by setting ACCUMULO_JAVA_OPTS to an array of options.
JAVA_OPTS=("${ACCUMULO_JAVA_OPTS[@]}" 
  '-XX:+UseConcMarkSweepGC'
  '-XX:CMSInitiatingOccupancyFraction=75'
  '-XX:+CMSClassUnloadingEnabled'
  '-XX:OnOutOfMemoryError=kill -9 %p'
  '-XX:-OmitStackTraceInFastThrow'
  '-Djava.net.preferIPv4Stack=true'
  "-Daccumulo.native.lib.path=${lib}/native"
  "-Daccumulo.dynamic.classpaths=${lib}/ext/[^.].*.jar")

## Make sure Accumulo native libraries are built since they are enabled by default
${bin}/accumulo-util build-native &> /dev/null

## JVM options set for individual applications
case "$cmd" in
master)  JAVA_OPTS=("${JAVA_OPTS[@]}" ${masterHigh_masterLow}) ;;
monitor) JAVA_OPTS=("${JAVA_OPTS[@]}" ${monitorHigh_monitorLow}) ;;
gc)      JAVA_OPTS=("${JAVA_OPTS[@]}" ${gcHigh_gcLow}) ;;
tserver) JAVA_OPTS=("${JAVA_OPTS[@]}" ${tServerHigh_tServerLow}) ;;
shell)   JAVA_OPTS=("${JAVA_OPTS[@]}" ${shellHigh_shellLow}) ;;
*)       JAVA_OPTS=("${JAVA_OPTS[@]}" ${otherHigh_otherLow}) ;;
esac

## JVM options set for logging.  Review logj4 properties files to see how they are used.
JAVA_OPTS=("${JAVA_OPTS[@]}" 
  "-Daccumulo.log.dir=${ACCUMULO_LOG_DIR}"
  "-Daccumulo.service.id=${cmd}${ACCUMULO_SERVICE_INSTANCE}_$(hostname)"
  "-Daccumulo.audit.log=$(hostname).audit")

case "$cmd" in
monitor)                    JAVA_OPTS=("${JAVA_OPTS[@]}" "-Dlog4j.configuration=file:${conf}/log4j-monitor.properties") ;;
gc|master|tserver|tracer)   JAVA_OPTS=("${JAVA_OPTS[@]}" "-Dlog4j.configuration=file:${conf}/log4j-service.properties") ;;
*)                          JAVA_OPTS=("${JAVA_OPTS[@]}" "-Dlog4j.configuration=file:${conf}/log4j.properties") ;;
esac

export JAVA_OPTS

############################
# Variables set to a default
############################

export MALLOC_ARENA_MAX=${MALLOC_ARENA_MAX:-1}
## Add Hadoop native libraries to shared library paths given operating system
case "$(uname)" in 
Darwin) export DYLD_LIBRARY_PATH="${HADOOP_PREFIX}/lib/native:${DYLD_LIBRARY_PATH}" ;; 
*)      export LD_LIBRARY_PATH="${HADOOP_PREFIX}/lib/native:${LD_LIBRARY_PATH}" ;;
esac

###############################################
# Variables that are optional. Uncomment to set
###############################################

## Specifies command that will be placed before calls to Java in accumulo script
# export ACCUMULO_JAVA_PREFIX=""
