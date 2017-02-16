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

############################
# Variables that must be set
############################

export ACCUMULO_TSERVER_OPTS="${tServerHigh_tServerLow} "
export ACCUMULO_MASTER_OPTS="${masterHigh_masterLow}"
export ACCUMULO_MONITOR_OPTS="${monitorHigh_monitorLow}"
export ACCUMULO_GC_OPTS="${gcHigh_gcLow}"
export ACCUMULO_SHELL_OPTS="${shellHigh_shellLow}"
export ACCUMULO_GENERAL_OPTS="-XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=75 -Djava.net.preferIPv4Stack=true -XX:+CMSClassUnloadingEnabled"
export ACCUMULO_OTHER_OPTS="${otherHigh_otherLow}"

#######################################
# Variables that are derived if not set
#######################################

## If not set below, Accumulo will settings in environment or derive
## locations using by looking up java, hadoop, zkiCli.sh on PATH

# export JAVA_HOME=/path/to/java
# export HADOOP_PREFIX=/path/to/hadoop
# export HADOOP_CONF_DIR=/path/to/hadoop/etc/conf
# export ZOOKEEPER_HOME=/path/to/zookeeper

## If not set below, Accumulo will derive these locations by determining the root of your
## installation and using the default locations

# export ACCUMULO_LOG_DIR=/path/to/accumulo/log

####################################################
# Variables that have a default. Uncomment to change
####################################################

## Specifies what do when the JVM runs out of heap memory
# export ACCUMULO_KILL_CMD='kill -9 %p'
## Should the monitor bind to all network interfaces
# export ACCUMULO_MONITOR_BIND_ALL="true"

###############################################
# Variables that are optional. Uncomment to set
###############################################

## Specifies command that will wrap calls to Java in bin/accumulo
# export ACCUMULO_WRAP_CMD=""
## Optionally look for hadoop and accumulo native libraries for your platform in additional
## directories. (Use DYLD_LIBRARY_PATH on Mac OS X.) May not be necessary for Hadoop 2.x or
## using an RPM that installs to the correct system library directory.
# export LD_LIBRARY_PATH=${HADOOP_PREFIX}/lib/native/${PLATFORM}:${LD_LIBRARY_PATH}
