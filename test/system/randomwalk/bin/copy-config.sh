#!/usr/bin/env bash

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


if [ -z $HADOOP_HOME ] ; then
    echo "HADOOP_HOME is not set.  Please make sure it's set globally."
    exit 1
fi

if [ -z $ACCUMULO_HOME ] ; then
    echo "ACCUMULO_HOME is not set.  Please make sure it's set globally."
    exit 1
fi

RW_HOME=$ACCUMULO_HOME/test/system/randomwalk

cd $RW_HOME

tar czf config.tgz conf
$HADOOP_HOME/bin/hadoop fs -rmr /randomwalk 2>/dev/null
$HADOOP_HOME/bin/hadoop fs -mkdir /randomwalk
$HADOOP_HOME/bin/hadoop fs -put config.tgz /randomwalk
$HADOOP_HOME/bin/hadoop fs -setrep 3 /randomwalk/config.tgz
rm config.tgz
