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

mkdir -p "$CONTINUOUS_LOG_DIR"

LOG_BASE="${CONTINUOUS_LOG_DIR}/$(date +%Y%m%d%H%M%S)_$(hostname)"

# Start agitators for datanodes, tservers, and the master
[[ -n $AGITATOR_USER ]] || AGITATOR_USER=$(whoami)
if [[ $AGITATOR_USER == root ]];  then
  echo "Running master-agitator and tserver-agitator as $ACCUMULO_USER using su. Running datanode-agitator as $HDFS_USER using su."

  # Change to the correct user if started as root
  su -c "nohup ${bin}/master-agitator.pl $MASTER_KILL_SLEEP_TIME $MASTER_RESTART_SLEEP_TIME >${LOG_BASE}_master-agitator.out 2>${LOG_BASE}_master-agitator.err &" -m - "$ACCUMULO_USER"

  su -c "nohup ${bin}/tserver-agitator.pl $TSERVER_KILL_SLEEP_TIME $TSERVER_RESTART_SLEEP_TIME $TSERVER_MIN_KILL $TSERVER_MAX_KILL >${LOG_BASE}_tserver-agitator.out 2>${LOG_BASE}_tserver-agitator.err &" -m - "$ACCUMULO_USER"

  su -c "nohup ${bin}/datanode-agitator.pl $DATANODE_KILL_SLEEP_TIME $DATANODE_RESTART_SLEEP_TIME $HADOOP_PREFIX $DATANODE_MIN_KILL $DATANODE_MAX_KILL >${LOG_BASE}_datanode-agitator.out 2>${LOG_BASE}_datanode-agitator.err &" -m - "$HDFS_USER"

elif [[ $AGITATOR_USER == "$ACCUMULO_USER" ]]; then
  echo "Running master-agitator and tserver-agitator as $AGITATOR_USER Running datanode-agitator as $HDFS_USER using sudo."
  # Just run the master-agitator if we're the accumulo user
  nohup "${bin}/master-agitator.pl" "$MASTER_KILL_SLEEP_TIME" "$MASTER_RESTART_SLEEP_TIME" >"${LOG_BASE}_master-agitator.out" 2>"${LOG_BASE}_master-agitator.err" &

  nohup "${bin}/tserver-agitator.pl" "$TSERVER_KILL_SLEEP_TIME" "$TSERVER_RESTART_SLEEP_TIME" "$TSERVER_MIN_KILL" "$TSERVER_MAX_KILL" >"${LOG_BASE}_tserver-agitator.out" 2>"${LOG_BASE}_tserver-agitator.err" &

  sudo -u "$HDFS_USER" nohup "${bin}/datanode-agitator.pl" "$DATANODE_KILL_SLEEP_TIME" "$DATANODE_RESTART_SLEEP_TIME" "$HADOOP_PREFIX" "$DATANODE_MIN_KILL" "$DATANODE_MAX_KILL" >"${LOG_BASE}_datanode-agitator.out" 2>"${LOG_BASE}_datanode-agitator.err" &

else
  echo "Running master-agitator and tserver-agitator as $ACCUMULO_USER using sudo. Running datanode-agitator as $HDFS_USER using sudo."

  # Not root, and not the accumulo user, hope you can sudo to it
  sudo -u "$ACCUMULO_USER" "nohup ${bin}/master-agitator.pl $MASTER_KILL_SLEEP_TIME $MASTER_RESTART_SLEEP_TIME >${LOG_BASE}_master-agitator.out 2>${LOG_BASE}_master-agitator.err &"

  sudo -u "$ACCUMULO_USER" "nohup ${bin}/tserver-agitator.pl $TSERVER_KILL_SLEEP_TIME $TSERVER_RESTART_SLEEP_TIME $TSERVER_MIN_KILL $TSERVER_MAX_KILL >${LOG_BASE}_tserver-agitator.out 2>${LOG_BASE}_tserver-agitator.err &"

  sudo -u "$HDFS_USER" "nohup ${bin}/datanode-agitator.pl $DATANODE_KILL_SLEEP_TIME $DATANODE_RESTART_SLEEP_TIME $HADOOP_PREFIX $DATANODE_MIN_KILL $DATANODE_MAX_KILL >${LOG_BASE}_datanode-agitator.out 2>${LOG_BASE}_datanode-agitator.err &" -m - "$HDFS_USER"

fi

if ${AGITATE_HDFS:-false} ; then
  AGITATOR_LOG=${LOG_BASE}_hdfs-agitator
  sudo -u "$AGITATE_HDFS_SUPERUSER" nohup "${bin}/hdfs-agitator.pl" --sleep "${AGITATE_HDFS_SLEEP_TIME}" --hdfs-cmd "${AGITATE_HDFS_COMMAND}" --superuser "${AGITATE_HDFS_SUPERUSER}" >"${AGITATOR_LOG}.out" 2>"${AGITATOR_LOG}.err" &
fi
