<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

Distributed Replication Test
===========================

Ingests random data into a table configured for replication, and then
verifies that the original table and the replicated table have equivalent
data using a Merkle tree.

* Steps to run test

1. Configure merkle-env.sh

    Be sure to set the username/password to connect to Accumulo and
    the Accumulo instance name and ZooKeepers. The example defaults
    to using the same instance as the source and destination.

    The randomness of the generated data can be controlled via some
    parameters in this file. Note that if deletes are introduced, it
    is very likely to cause an incorrect verification since the tombstone'ing
    of that delete will differ on the source and remote due to the order
    of minor compactions that occur.

2. Run configure-replication.sh

    This script sets up the two instances for replication, creates
    the tables that will be replicated and adds some splits to them.

    This is destructive to any existing replication configuration so
    use it with care on a real instance.

3. Run ingest-data.sh

    Ingests the configured amount of random data into the source
    table.

4. Run 'accumulo-cluster stop' && 'accumulo-cluster start' on the source instance

    A tabletserver in the source instance is likely to still be referencing
    a WAL for a presently online tablet which will prevent that
    file from being replicated. Stopping the local instance will ensure
    that all WALs are candidate for replication.

5. Run verify-data.sh

    This will compute the leaves merkle tree for the source and destination
    tables and then compute the root hash for both. The root hash
    is presented to the user.

    If the root hashes are equal, the test passed; otherwise, the test fails.
