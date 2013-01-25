#! /bin/sh
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

if [ $(id -ur) -ne 0 ]; then
  echo "This script must be run as root" 1>&2
  exit 1
fi

if [ ! -f /etc/default/accumulo ]; then
  mkdir -p /etc/default
  touch /etc/default/accumulo
fi

if ! grep "ACCUMULO_TRACER_USER=" /etc/default/accumulo  >> /dev/null ; then
  echo "ACCUMULO_TRACER_USER=accumulo_tracer" >> /etc/default/accumulo
fi
 
if ! id -u accumulo_tracer >/dev/null 2>&1; then
  if ! egrep "^accumulo:" /etc/group >> /dev/null; then
    groupadd accumulo
  fi 
  useradd -d /usr/lib/accumulo -g accumulo accumulo_tracer
fi

install -m 0755 -o root -g root init.d/accumulo-tracer /etc/init.d/
if [ -e "`which update-rc.d`" ]; then 
  update-rc.d accumulo-tracer start 22 2 3 4 5 . stop 20 0 1 6 .
elif [ -e "`which chkconfig`" ]; then
  chkconfig --add accumulo-tracer
else
  echo "No update-rc.d or chkconfig, rc levels not set for accumulo-tracer"
fi
