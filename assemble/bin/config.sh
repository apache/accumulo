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

# Guarantees that Accumulo and its environment variables are set.
#
# Parameters checked by script
#  ACCUMULO_VERIFY_ONLY set to skip actions that would alter the local filesystem
#
# Values set by script that can be user provided.  If not provided script attempts to infer.
#  ACCUMULO_CONF_DIR  Location where accumulo-env.sh, accumulo-site.xml and friends will be read from
#  ACCUMULO_HOME      Home directory for Accumulo
#  ACCUMULO_LOG_DIR   Directory for Accumulo daemon logs
#  ACCUMULO_VERSION   Accumulo version name
#  HADOOP_PREFIX      Prefix to the home dir for hadoop.
#
# Iff ACCUMULO_VERIFY_ONLY is not set, this script will
#   * Ensure the existence of ACCUMULO_LOG_DIR on the current host
#
# Values always set by script.
#  MALLOC_ARENA_MAX   To work around a memory management bug (see ACCUMULO-847)
#  HADOOP_HOME        Home dir for hadoop.  TODO fix this.
#
# Values set by script if certain files exist
# ACCUMULO_JAAS_CONF  Location of jaas.conf file. Needed by JAAS for things like Kerberos based logins
# ACCUMULO_KRB5_CONF  Location of krb5.conf file. Needed by Kerberos subsystems to find login servers
#
# NUMA related environment variables
# ACCUMULO_ENABLE_NUMACTL   (Default: true) Use numactl if present on the system
# ACCUMULO_NUMACTL_OPTIONS  (Default: --interleave=all) options to the numactl command

if [ -z "${ACCUMULO_HOME}" ] ; then
  # Start: Resolve Script Directory
  SOURCE="${BASH_SOURCE[0]}"
  while [[ -h $SOURCE ]]; do # resolve $SOURCE until the file is no longer a symlink
     bin=$( cd -P "$( dirname "$SOURCE" )" && pwd )
     SOURCE=$(readlink "$SOURCE")
     [[ $SOURCE != /* ]] && SOURCE="$bin/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
  done
  bin=$( cd -P "$( dirname "$SOURCE" )" && pwd )
  script=$( basename "$SOURCE" )
  # Stop: Resolve Script Directory

  ACCUMULO_HOME=$( cd -P "${bin}"/.. && pwd )
  export ACCUMULO_HOME
fi

if [[ ! -d ${ACCUMULO_HOME} ]]; then
  echo "ACCUMULO_HOME=${ACCUMULO_HOME} is not a valid directory. Please make sure it exists"
  exit 1
fi

export ACCUMULO_CONF_DIR="${ACCUMULO_CONF_DIR:-$ACCUMULO_HOME/conf}"
if [[ -z $ACCUMULO_CONF_DIR || ! -d $ACCUMULO_CONF_DIR ]]
then
  echo "ACCUMULO_CONF_DIR=$ACCUMULO_CONF_DIR is not a valid directory.  Please make sure it exists"
  exit 1
fi

if [[ -f $ACCUMULO_CONF_DIR/accumulo-env.sh ]] ; then
   . "$ACCUMULO_CONF_DIR/accumulo-env.sh"
elif [[ -z $ACCUMULO_TEST ]] ; then
   #
   # Attempt to bootstrap configuration and continue
   #
   echo
   echo 'Accumulo is not properly configured.'
   echo
   echo 'Try running $ACCUMULO_HOME/bin/bootstrap_config.sh and then editing'
   echo '$ACCUMULO_HOME/conf/accumulo-env.sh'
   echo
   exit 1
fi

if [[ -f $ACCUMULO_CONF_DIR/slaves ]]; then
  echo "ERROR: A 'slaves' file was found in $ACCUMULO_CONF_DIR/"
  echo "Accumulo now reads tablet server hosts from 'tservers' and requires that the 'slaves' file not be present to reduce confusion."
  echo "Please rename the 'slaves' file to 'tservers' or remove it if both exist."
  exit 1
fi

if [[ ! -f $ACCUMULO_CONF_DIR/tservers ]]; then
  echo "ERROR: A 'tservers' file was not found at $ACCUMULO_CONF_DIR/tservers"
  echo "Please make sure it exists and is configured with tablet server hosts."
  exit 1
fi

[[ -z $ACCUMULO_LOG_DIR ]] && ACCUMULO_LOG_DIR=$ACCUMULO_HOME/logs
[[ -z $ACCUMULO_VERIFY_ONLY ]] && mkdir -p $ACCUMULO_LOG_DIR 2>/dev/null

export ACCUMULO_LOG_DIR

if [[ -z "$HADOOP_PREFIX" ]]
then
   HADOOP_PREFIX="$(which hadoop)"
   if [[ -z "$HADOOP_PREFIX" ]]
   then
      echo "You must set HADOOP_PREFIX"
      exit 1
   fi
   HADOOP_PREFIX=$(dirname "$HADOOP_PREFIX")
   HADOOP_PREFIX=$(dirname "$HADOOP_PREFIX")
fi
if [[ ! -d "$HADOOP_PREFIX" ]]
then
   echo "HADOOP_PREFIX, which has a value of $HADOOP_PREFIX, is not a directory."
   exit 1
fi
export HADOOP_PREFIX

ACCUMULO_ENABLE_NUMACTL=${ACCUMULO_ENABLE_NUMACTL:-"true"}
ACCUMULO_NUMACTL_OPTIONS=${ACCUMULO_NUMACTL_OPTIONS:-"--interleave=all"}
NUMA=`which numactl 2>/dev/null`
NUMACTL_EXISTS="$?"
if [[ ( ${NUMACTL_EXISTS} -eq 0 ) && ( ${ACCUMULO_ENABLE_NUMACTL} == "true" ) ]] ; then
  export NUMA_CMD="${NUMA} ${ACCUMULO_NUMACTL_OPTIONS}"
else
  export NUMA_CMD=""
fi

NUM_TSERVERS=${NUM_TSERVERS:-1}

# Validate that NUM_TSERVERS is a positive integer
if ! [[ $NUM_TSERVERS =~ ^[0-9]+$ ]]; then
   echo "NUM_TSERVERS, when defined in accumulo-env.sh, should be a positive number, is '$NUM_TSERVERS'"
   exit 1
fi

# NUMA sanity checks
if [[ $NUM_TSERVERS -eq 1 && -n $TSERVER_NUMA_OPTIONS ]]; then
   echo "TSERVER_NUMA_OPTIONS declared when NUM_TSERVERS is 1, use ACCUMULO_NUMACTL_OPTIONS instead"
   exit 1
fi
if [[ $NUM_TSERVERS -gt 1 && -n $TSERVER_NUMA_OPTIONS && ${#TSERVER_NUMA_OPTIONS[*]} -ne $NUM_TSERVERS ]]; then
   echo "TSERVER_NUMA_OPTIONS is declared, but not the same size as NUM_TSERVERS"
   exit 1
fi

export HADOOP_HOME=$HADOOP_PREFIX
export HADOOP_HOME_WARN_SUPPRESS=true

# See HADOOP-7154 and ACCUMULO-847
export MALLOC_ARENA_MAX=${MALLOC_ARENA_MAX:-1}

# Check for jaas.conf configuration
if [[ -z ${ACCUMULO_JAAS_CONF} ]]; then
  if [[ -f ${ACCUMULO_CONF_DIR}/jaas.conf ]]; then
    export ACCUMULO_JAAS_CONF=${ACCUMULO_CONF_DIR}/jaas.conf
  fi
fi

# Check for krb5.conf configuration
if [[ -z ${ACCUMULO_KRB5_CONF} ]]; then
  if [[ -f ${ACCUMULO_CONF_DIR}/krb5.conf ]]; then
    export ACCUMULO_KRB5_CONF=${ACCUMULO_CONF_DIR}/krb5.conf
  fi
fi


