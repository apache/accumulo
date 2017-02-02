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

# Sources accumulo-env.sh and verifies environment variables

function verify_env_dir() {
  property=$1
  directory=$2
  if [[ -z "$directory" ]]; then
    echo "$property is not set. Please make sure it's set globally or in conf/accumulo-env.sh."
    exit 1
  fi
  if [[ ! -d "$directory" ]]; then
    echo "$property=$directory is not a valid directory. Please make sure it's set correctly globally or in conf/accumulo-env.sh."
    exit 1
  fi
}

# Resolve a program to its installation directory
locationByProgram()
{
   RESULT=$( which "$1" )
   if [[ "$?" != 0 && -z "${RESULT}" ]]; then
      echo "Cannot find '$1' and '$2' is not set in $conf/accumulo-env.sh"
      exit 1
   fi
   while [ -h "${RESULT}" ]; do # resolve $RESULT until the file is no longer a symlink
      DIR="$( cd -P "$( dirname "$RESULT" )" && pwd )"
      RESULT="$(readlink "${RESULT}")"
      [[ "${RESULT}" != /* ]] && RESULT="${DIR}/${RESULT}" # if $RESULT was a relative symlink, we need to resolve it relative to the path where the symlink file was located
   done
   # find the relative home directory, accounting for an extra bin directory
   RESULT=$(dirname "$(dirname "${RESULT}")")
   echo "Auto-set ${2} to '${RESULT}'.  To suppress this message, set ${2} in conf/accumulo-env.sh"
   eval "${2}=${RESULT}"
}

# Resolve base directory
SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do
   libexec="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
   SOURCE="$(readlink "$SOURCE")"
   [[ $SOURCE != /* ]] && SOURCE="$libexec/$SOURCE"
done
libexec="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
basedir=$( cd -P "${libexec}"/.. && pwd )
conf="${basedir}/conf"

if [[ -z $conf || ! -d $conf ]]; then
  echo "$conf is not a valid directory.  Please make sure it exists"
  exit 1
fi

if [[ ! -f $conf/accumulo-env.sh || ! -f $conf/accumulo-site.xml ]]; then
  echo "The configuration files 'accumulo-env.sh' & 'accumulo-site.xml' must exist in $conf"
  echo "Run 'accumulo create-config' to create them or copy them from $conf/examples"
  echo "Follow the instructions in INSTALL.md to edit them for your environment."
  exit 1
fi

source "$conf/accumulo-env.sh"

## Variables that must be set

: "${ACCUMULO_TSERVER_OPTS:?"variable is not set in accumulo-env.sh"}"
: "${ACCUMULO_MASTER_OPTS:?"variable is not set in accumulo-env.sh"}"
: "${ACCUMULO_MONITOR_OPTS:?"variable is not set in accumulo-env.sh"}"
: "${ACCUMULO_GC_OPTS:?"variable is not set in accumulo-env.sh"}"
: "${ACCUMULO_SHELL_OPTS:?"variable is not set in accumulo-env.sh"}"
: "${ACCUMULO_GENERAL_OPTS:?"variable is not set in accumulo-env.sh"}"
: "${ACCUMULO_OTHER_OPTS:?"variable is not set in accumulo-env.sh"}"

### Variables that are derived

# If not set in accumulo-env.sh, set env variables by program location.
test -z "${JAVA_HOME}" && locationByProgram java JAVA_HOME
test -z "${HADOOP_PREFIX}" && locationByProgram hadoop HADOOP_PREFIX
test -z "${ZOOKEEPER_HOME}" && locationByProgram zkCli.sh ZOOKEEPER_HOME

export HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-$HADOOP_PREFIX/etc/hadoop}"
export ACCUMULO_HOME="$basedir"
export ACCUMULO_CONF_DIR="$conf"
export ACCUMULO_LOG_DIR="${ACCUMULO_LOG_DIR:-$basedir/logs}"

# Make directories that may not exist
mkdir -p "${ACCUMULO_LOG_DIR}" 2>/dev/null
mkdir -p "${basedir}/run" 2>/dev/null

# Verify all directories exist
verify_env_dir "JAVA_HOME" "${JAVA_HOME}"
verify_env_dir "HADOOP_PREFIX" "${HADOOP_PREFIX}"
verify_env_dir "HADOOP_CONF_DIR" "${HADOOP_CONF_DIR}"
verify_env_dir "ZOOKEEPER_HOME" "${ZOOKEEPER_HOME}"

## Verify Zookeeper installation
ZOOKEEPER_VERSION=$(find -L "$ZOOKEEPER_HOME" -maxdepth 1 -name "zookeeper-[0-9]*.jar" | head -1)
if [ -z "$ZOOKEEPER_VERSION" ]; then
  echo "A Zookeeper JAR was not found in $ZOOKEEPER_HOME."
  echo "Please check ZOOKEEPER_HOME, either globally or in accumulo-env.sh."
  exit 1
fi
ZOOKEEPER_VERSION=$(basename "${ZOOKEEPER_VERSION##*-}" .jar)

if [[ "$ZOOKEEPER_VERSION" < "3.4.0" ]]; then
  echo "WARN : Using Zookeeper $ZOOKEEPER_VERSION.  Use version 3.4.0 or greater. Older versions may not work reliably.";
fi

## Variables that have a default
export ACCUMULO_KILL_CMD=${ACCUMULO_KILL_CMD:-'kill -9 %p'}
export ACCUMULO_MONITOR_BIND_ALL=${ACCUMULO_MONITOR_BIND_ALL:-"true"}

export HADOOP_HOME=$HADOOP_PREFIX
export HADOOP_HOME_WARN_SUPPRESS=true

# See HADOOP-7154 and ACCUMULO-847
export MALLOC_ARENA_MAX=${MALLOC_ARENA_MAX:-1}
