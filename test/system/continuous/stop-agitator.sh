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
while [[ -h "${SOURCE}" ]]; do # resolve $SOURCE until the file is no longer a symlink
   bin=$( cd -P "$( dirname "${SOURCE}" )" && pwd )
   SOURCE=$(readlink "${SOURCE}")
   [[ "${SOURCE}" != /* ]] && SOURCE="${bin}/${SOURCE}" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
bin=$( cd -P "$( dirname "${SOURCE}" )" && pwd )
script=$( basename "${SOURCE}" )
# Stop: Resolve Script Directory

CONTINUOUS_CONF_DIR=${CONTINUOUS_CONF_DIR:-${bin}}
. "$CONTINUOUS_CONF_DIR/continuous-env.sh"

# Try to use sudo when we wouldn't normally be able to kill the processes
[[ -n $AGITATOR_USER ]] || AGITATOR_USER=$(whoami)
if [[ $AGITATOR_USER == root ]]; then
  echo "Stopping all processes matching 'agitator.pl' as root"
  pkill -f agitator.pl 2>/dev/null
elif [[ $AGITATOR_USER == "$ACCUMULO_USER" ]];  then
  echo "Stopping all processes matching 'datanode-agitator.pl' as $HDFS_USER"
  sudo -u "$HDFS_USER" pkill -f datanode-agitator.pl 2>/dev/null
  echo "Stopping all processes matching 'hdfs-agitator.pl' as $HDFS_USER"
  sudo -u "$HDFS_USER" pkill -f hdfs-agitator.pl 2>/dev/null
  echo "Stopping all processes matching 'agitator.pl' as $AGITATOR_USER"
  pkill -f agitator.pl 2>/dev/null 2>/dev/null
else
  echo "Stopping all processes matching 'datanode-agitator.pl' as $HDFS_USER"
  sudo -u "$HDFS_USER" pkill -f datanode-agitator.pl 2>/dev/null
  echo "Stopping all processes matching 'hdfs-agitator.pl' as $HDFS_USER"
  sudo -u "$HDFS_USER" pkill -f hdfs-agitator.pl 2>/dev/null
  echo "Stopping all processes matching 'agitator.pl' as $ACCUMULO_USER"
  sudo -u "$ACCUMULO_USER" pkill -f agitator.pl 2>/dev/null
fi

