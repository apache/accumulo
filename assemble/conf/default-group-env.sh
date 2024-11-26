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

# This script is called from accumulo-service and serves as a mechanism to set
# ACCUMULO_JAVA_OPTS and provide property overrides for the server processes in
# a resource group. This allows the user to set a base configuration in
# accumulo-env.sh and accumulo.properties and then tailor the configuration of
# the individual processes in a resource group. For example, when you have a
# cluster comprised of sets of servers with different amounts of memory, then
# the user can set default JVM heap settings in accumulo-env.sh, and then
# increase or decrease in this file.
# 
# This script will export ACCUMULO_JAVA_OPTS and PROPERTY_OVERRIDES variables
# based on the service type for the default resource group.  Additional files can
# be created based on the groups configured in cluster.yaml and should be named
# "{groupName}-group-env.sh". The contents of ACCUMULO_JAVA_OPTS will be appended
# to the JAVA_OPTS variable that is created in accumulo-env.sh, allowing the user
# to override any setting for this resource group. Likewise, the contents of 
# PROPERTY_OVERRIDES will be appended to the arguments provided to the server
# class allowing the user to override any Accumulo property for this resource group.
# Overriding the bind address and group name properties would not be advisable as
# this could lead to deployment issues.

#  Examples:
#
#    Override JVM args
#    export ACCUMULO_JAVA_OPTS="-Xms1024m -Xmx1024m"
#
#    Override Accumulo properties
#    export PROPERTY_OVERRIDES=('-o' 'rpc.backlog=1000' '-o' 'tserver.hold.time.max=10m')

service=$1

case "$service" in
  manager)
    export ACCUMULO_JAVA_OPTS=""
    export PROPERTY_OVERRIDES=()
    ;;
  monitor)
    export ACCUMULO_JAVA_OPTS=""
    export PROPERTY_OVERRIDES=()
    ;;
  gc)
    export ACCUMULO_JAVA_OPTS=""
    export PROPERTY_OVERRIDES=()
    ;;
  tserver)
    export ACCUMULO_JAVA_OPTS=""
    export PROPERTY_OVERRIDES=()
    ;;
  compactor)
    export ACCUMULO_JAVA_OPTS=""
    export PROPERTY_OVERRIDES=()
    ;;
  sserver)
    export ACCUMULO_JAVA_OPTS=""
    export PROPERTY_OVERRIDES=()
    ;;
  *)
    export ACCUMULO_JAVA_OPTS=""
    export PROPERTY_OVERRIDES=()
    ;;
esac
