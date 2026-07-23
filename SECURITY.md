<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

## Reporting a vulnerability

If after reading this document you believe you have found a vulnerability or are unsure,
then please follow the [reporting process](https://www.apache.org/security/#reporting-a-vulnerability).

## High-Level Accumulo Deployment Overview

Accumulo is designed to use Apache ZooKeeper for locking and coordination 
and one or more Hadoop Compatible File System (HCFS) implementations 
(HDFS, S3, local disk, etc.) for storage. Accumulo users could be humans 
or machines connecting to Accumulo server processes from any location. 
Please see the Security section of the User Manual for information 
regarding:

  * User authentication
  * Authorizing actions and our permission scheme
  * Labeling data and scan-time authorizations
  * Encryption

## Security Assumptions

The following items are out of our control and we assume that these items
are configured correctly and securely or are otherwise trusted.

  * Network, to include network encryption components
  * DNS
  * Operating Systems and related compute hardware
  * ZooKeeper and HCFS storage configured for Accumulo's use
  * Persons or processes that can log into an environment (bare-metal or container)
    where Accumulo, ZooKeeper, or HCFS-related processes are running
  * Accumulo configuration files (e.g. accumulo-env.sh, accumulo.properties, logging configuration, etc.)
  * The JVM classpath for Accumulo processes (configured in accumulo-env.sh or similar mechanism)
  * User provided code that runs in the server process via an Accumulo SPI
  * User configured JVM agents
  
## Accumulo Security Boundary and Responsibilities

The Accumulo RPC layer, not the Accumulo client or shell, is the security boundary. Users
are not required to use the client and may interact with server processes via
supported RPC's. Security enforcement mechanisms must be implemented on the
server-side. Additional enforcement mechanisms may be implemented in the client,
but are not required. Accumulo's security relevant requirements are below:

  * To authenticate users at the RPC layer
  * To authorize the users action against the relevant permission, if applicable, at the RPC layer
  * To ensure that a user can only use Authorizations that they are assigned in a Scanner/BatchScanner
  * To ensure that Key-Value pairs returned in a Scanner/BatchScanner are allowed given the scan Authorizations
  * To ensure that files are encrypted/decrypted appropriately, if configured
  
## Additional Considerations

  * Accumulo server logs will contain user data and should be secured
  * The Accumulo Monitor web UI does not perform authorization, user data should not be passed over the REST endpoints
  * Each entry in a bulk-imported file is NOT inspected to confirm that the visiblity label is valid
  
  