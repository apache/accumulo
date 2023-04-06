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

## Before accumulo-env.sh is loaded, these environment variables are set and can be used in this file:

# cmd - Command that is being called such as tserver, manager, etc.
# basedir - Root of Accumulo installation
# bin - Directory containing Accumulo scripts
# conf - Directory containing Accumulo configuration
# lib - Directory containing Accumulo libraries

############################
# Variables that must be set
############################

## Accumulo logs directory. Referenced by logger config.
ACCUMULO_LOG_DIR="${ACCUMULO_LOG_DIR:-${basedir}/logs}"
## Hadoop installation
HADOOP_HOME="${HADOOP_HOME:-/path/to/hadoop}"
## Hadoop configuration
HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-${HADOOP_HOME}/etc/hadoop}"
## Zookeeper installation
ZOOKEEPER_HOME="${ZOOKEEPER_HOME:-/path/to/zookeeper}"

##########################
# Build CLASSPATH variable
##########################

## Verify that Hadoop & Zookeeper installation directories exist
if [[ ! -d $ZOOKEEPER_HOME ]]; then
  echo "ZOOKEEPER_HOME=$ZOOKEEPER_HOME is not set to a valid directory in accumulo-env.sh"
  exit 1
fi
if [[ ! -d $HADOOP_HOME ]]; then
  echo "HADOOP_HOME=$HADOOP_HOME is not set to a valid directory in accumulo-env.sh"
  exit 1
fi

## Build using existing CLASSPATH, conf/ directory, dependencies in lib/, and external Hadoop & Zookeeper dependencies
if [[ -n $CLASSPATH ]]; then
  # conf is set by calling script that sources this env file
  #shellcheck disable=SC2154
  CLASSPATH="${CLASSPATH}:${conf}"
else
  CLASSPATH="${conf}"
fi
ZK_JARS=$(find "$ZOOKEEPER_HOME/lib/" -maxdepth 1 -name '*.jar' -not -name '*slf4j*' -not -name '*log4j*' | paste -sd:)
# lib is set by calling script that sources this env file
#shellcheck disable=SC2154
CLASSPATH="${CLASSPATH}:${lib}/*:${HADOOP_CONF_DIR}:${ZOOKEEPER_HOME}/*:${ZK_JARS}:${HADOOP_HOME}/share/hadoop/client/*"
export CLASSPATH

##################################################################
# Build JAVA_OPTS variable. Defaults below work but can be edited.
##################################################################

## JVM options set for all processes. Extra options can be passed in by setting ACCUMULO_JAVA_OPTS to an array of options.
read -r -a accumulo_initial_opts < <(echo "$ACCUMULO_JAVA_OPTS")
JAVA_OPTS=(
  '-XX:OnOutOfMemoryError=kill -9 %p'
  '-XX:-OmitStackTraceInFastThrow'
  '-Djava.net.preferIPv4Stack=true'
  "-Daccumulo.native.lib.path=${lib}/native"
  "${accumulo_initial_opts[@]}"
)

## Make sure Accumulo native libraries are built since they are enabled by default
# bin is set by calling script that sources this env file
#shellcheck disable=SC2154
"${bin}"/accumulo-util build-native &>/dev/null

## JVM options set for individual applications
# cmd is set by calling script that sources this env file
#shellcheck disable=SC2154
case "$cmd" in
  manager | master) JAVA_OPTS=('-Xmx512m' '-Xms512m' "${JAVA_OPTS[@]}") ;;
  monitor) JAVA_OPTS=('-Xmx256m' '-Xms256m' "${JAVA_OPTS[@]}") ;;
  gc) JAVA_OPTS=('-Xmx256m' '-Xms256m' "${JAVA_OPTS[@]}") ;;
  tserver) JAVA_OPTS=('-Xmx768m' '-Xms768m' "${JAVA_OPTS[@]}") ;;
  compaction-coordinator) JAVA_OPTS=('-Xmx512m' '-Xms512m' "${JAVA_OPTS[@]}") ;;
  compactor) JAVA_OPTS=('-Xmx256m' '-Xms256m' "${JAVA_OPTS[@]}") ;;
  sserver) JAVA_OPTS=('-Xmx512m' '-Xms512m' "${JAVA_OPTS[@]}") ;;
  *) JAVA_OPTS=('-Xmx256m' '-Xms64m' "${JAVA_OPTS[@]}") ;;
esac

## JVM options set for logging. Review log4j2.properties file to see how they are used.
JAVA_OPTS=("-Daccumulo.log.dir=${ACCUMULO_LOG_DIR}"
  "-Daccumulo.application=${cmd}${ACCUMULO_SERVICE_INSTANCE}_$(hostname)"
  "-Daccumulo.metrics.service.instance=${ACCUMULO_SERVICE_INSTANCE}"
  "-Dlog4j2.contextSelector=org.apache.logging.log4j.core.async.AsyncLoggerContextSelector"
  "-Dotel.service.name=${cmd}${ACCUMULO_SERVICE_INSTANCE}"
  "${JAVA_OPTS[@]}"
)

## Optionally setup OpenTelemetry SDK AutoConfigure
## See https://github.com/open-telemetry/opentelemetry-java/tree/main/sdk-extensions/autoconfigure
#JAVA_OPTS=('-Dotel.traces.exporter=jaeger' '-Dotel.metrics.exporter=none' '-Dotel.logs.exporter=none' "${JAVA_OPTS[@]}")

## Optionally setup OpenTelemetry Java Agent
## See https://github.com/open-telemetry/opentelemetry-java-instrumentation for more options
#JAVA_OPTS=('-javaagent:path/to/opentelemetry-javaagent-all.jar' "${JAVA_OPTS[@]}")

case "$cmd" in
  monitor | gc | manager | master | tserver | compaction-coordinator | compactor | sserver)
    JAVA_OPTS=('-Dlog4j.configurationFile=log4j2-service.properties' "${JAVA_OPTS[@]}")
    ;;
  *)
    # let log4j use its default behavior (log4j2.properties, etc.)
    true
    ;;
esac

############################
# Variables set to a default
############################

export MALLOC_ARENA_MAX=${MALLOC_ARENA_MAX:-1}
## Add Hadoop native libraries to shared library paths given operating system
case "$(uname)" in
  Darwin) export DYLD_LIBRARY_PATH="${HADOOP_HOME}/lib/native:${DYLD_LIBRARY_PATH}" ;;
  *) export LD_LIBRARY_PATH="${HADOOP_HOME}/lib/native:${LD_LIBRARY_PATH}" ;;
esac

###############################################
# Variables that are optional. Uncomment to set
###############################################

## Specifies command that will be placed before calls to Java in accumulo script
# export ACCUMULO_JAVA_PREFIX=""
