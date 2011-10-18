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

OS=`uname -s`

#Validate arguments
if [ -z "$1" -o -z "$2" -o -z "$3" ]; then
echo "Usage: $0 <SIGNAL> <UID> <PATTERN>"
exit 1
fi

#Darwin settings (MacOS)
if [ $OS = Darwin ]; then
  ps -eo pid,command | grep "$3" | grep -v grep | awk '{ print $1 }' | grep -v $$ | xargs kill -$1

#Linux settings (RedHat)
elif [ $OS = Linux ]; then
  #echo pkill -$1 -U $2 -f "$3"
  exec pkill -$1 -U $2 -f "$3"

#Any other OS
else
  echo "Unrecognized OS"
  exit 1
#End OS checking
fi
