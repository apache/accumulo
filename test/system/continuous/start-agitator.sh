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

CONTINUOUS_CONF_DIR=${CONTINUOUS_CONF_DIR:-$ACCUMULO_HOME/test/system/continuous/}
. $CONTINUOUS_CONF_DIR/continuous-env.sh
export HADOOP_PREFIX

mkdir -p $CONTINUOUS_LOG_DIR

# Agitator needs to handle HDFS and Accumulo - can't switch to a single user and expect it to work
nohup ./agitator.pl $KILL_SLEEP_TIME $TUP_SLEEP_TIME $HDFS_USER $ACCUMULO_USER $MIN_KILL $MAX_KILL >$CONTINUOUS_LOG_DIR/`date +%Y%m%d%H%M%S`_`hostname`_agitator.out 2>$CONTINUOUS_LOG_DIR/`date +%Y%m%d%H%M%S`_`hostname`_agitator.err &

if [[ "`whoami`" == "root" ]];  then
  # Change to the correct user if started as root
  su -c "nohup $CONTINUOUS_CONF_DIR/magitator.pl $MASTER_KILL_SLEEP_TIME $MASTER_RESTART_SLEEP_TIME >$CONTINUOUS_LOG_DIR/`date +%Y%m%d%H%M%S`_`hostname`_magitator.out 2>$CONTINUOUS_LOG_DIR/`date +%Y%m%d%H%M%S`_`hostname`_magitator.err &" -m - $ACCUMULO_USER
elif [[ "`whoami`" == $ACCUMULO_USER ]]; then
  # Just run the magitator if we're the accumulo user
  nohup $CONTINUOUS_CONF_DIR/magitator.pl $MASTER_KILL_SLEEP_TIME $MASTER_RESTART_SLEEP_TIME >$CONTINUOUS_LOG_DIR/`date +%Y%m%d%H%M%S`_`hostname`_magitator.out 2>$CONTINUOUS_LOG_DIR/`date +%Y%m%d%H%M%S`_`hostname`_magitator.err &
else
  # Not root, and not the accumulo user, hope you can sudo to it
  sudo -m -u $ACCUMULO_USER "nohup $CONTINUOUS_CONF_DIR/magitator.pl $MASTER_KILL_SLEEP_TIME $MASTER_RESTART_SLEEP_TIME >$CONTINUOUS_LOG_DIR/`date +%Y%m%d%H%M%S`_`hostname`_magitator.out 2>$CONTINUOUS_LOG_DIR/`date +%Y%m%d%H%M%S`_`hostname`_magitator.err &"
fi

if ${AGITATE_HDFS:-false} ; then
  AGITATOR_LOG=${CONTINUOUS_LOG_DIR}/`date +%Y%m%d%H%M%S`_`hostname`_hdfs-agitator
  nohup ./hdfs-agitator.pl --sleep ${AGITATE_HDFS_SLEEP_TIME} --hdfs-cmd ${AGITATE_HDFS_COMMAND} --superuser ${AGITATE_HDFS_SUPERUSER} --sudo ${AGITATE_HDFS_SUDO} >${AGITATOR_LOG}.out 2>${AGITATOR_LOG}.err &
fi
