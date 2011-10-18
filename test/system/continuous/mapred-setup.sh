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

# a little helper script that other scripts can source to setup 
# for running a map reduce job

. continuous-env.sh
. $ACCUMULO_HOME/conf/accumulo-env.sh

SERVER_CMD='ls -1 $ACCUMULO_HOME/lib/accumulo-server-*[!javadoc\|sources].jar'

if [ `eval $SERVER_CMD | wc -l` != "1" ] ; then
    echo "Not exactly one accumulo-server jar in $ACCUMULO_HOME/lib"
    exit 1
fi

SERVER_LIBJAR=`eval $SERVER_CMD`
