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

# Edit the credentials to match your system
USERPASS='-u root -p secret'
INSTANCE='-z localhost -i inst'

# Edit these to change the range of each cell component. The size is in bytes.
ROW_RANGE='--min-row-size 128 --max-row-size 128'
CF_RANGE='--min-cf-size 128 --max-cf-size 128'
CQ_RANGE='--min-cq-size 128 --max-cq-size 128'
VALUE_RANGE='--min-value-size 1024 --max-value-size 2048'
ROW_WIDTH='--min-row-width 1 --max-row-width 10'

# These are the seeds for the random number generates used to generate each cell component.
ROW_SEED='--row-seed 1'
CF_SEED='--cf-seed 2'
CQ_SEED='--cq-seed 3'
VALUE_SEED='--value-seed 4'
ROW_WIDTH_SEED='--row-width-seed 5'

# This is the delay in milliseconds between writes. Use <= 0 for no delay.
WRITE_DELAY='--write-delay 0'

# Let's reset the table, for good measure
../../../bin/accumulo shell $USERPASS -e 'deletetable -f stress_test'
../../../bin/accumulo shell $USERPASS -e 'createtable stress_test'

../../../bin/accumulo org.apache.accumulo.test.stress.random.Write $INSTANCE $USERPASS $ROW_RANGE $CF_RANGE $CQ_RANGE $VALUE_RANGE \
  $ROW_SEED $CF_SEED $CQ_SEED $VALUE_SEED $ROW_WIDTH $ROW_WIDTH_SEED $WRITE_DELAY
