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

# This script will regenerate the example configuration files for the tarball

out=target/bootstrap-config.out

echo 'Generating example scripts...' > $out
for s in 1GB 2GB 3GB 512MB
do
  bin/bootstrap_config.sh -o -d target/example-configs/$s/standalone -s $s -j -v 2 >> $out 2>&1
  bin/bootstrap_config.sh -o -d target/example-configs/$s/native-standalone -s $s -n -v 2 >> $out 2>&1
done

