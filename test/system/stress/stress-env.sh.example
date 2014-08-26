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

ACCUMULO_HOME=${ACCUMULO_HOME:-/opt/accumulo}

# Edit the credentials to match your system
USERPASS='-u root -p secret'
INSTANCE='-z localhost -i inst'

### Read configuration ###

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

### Write configuration ###

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

# This is the maximum number of cells to include in each mutation written out.
# A non-positive value implies no limit.
MAX_CELLS_PER_MUTATION='--max-cells-per-mutation -1'

# This is the delay in milliseconds between writes. Use <= 0 for no delay.
WRITE_DELAY='--write-delay 0'
