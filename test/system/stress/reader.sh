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

# This is the seed for the range picking logic used by the scanner.
SCAN_SEED='--scan-seed 1337'

# Controls the number of random tablets the scanner will read sequentially
#SCAN_ITERATIONS='--num-iterations 1024'

# Alternatively, we can just continously scan
CONTINUOUS_SCAN='--continuous'

# Controls whether or not the scan will be using an isolated scanner. Add this to the execution 
#SCAN_ISOLATION='--isolate'

# Sets the batch size for the scanner, use a lower number for large rows / cells
#SCAN_BATCH_SIZE='--scan-batch-size -1'

../../../bin/accumulo org.apache.accumulo.test.stress.random.Scan $INSTANCE $USERPASS $SCAN_SEED $CONTINUOUS_SCAN \
  $SCAN_BATCH_SIZE
