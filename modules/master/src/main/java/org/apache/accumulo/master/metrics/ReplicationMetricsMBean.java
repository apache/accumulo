/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.master.metrics;

/**
 *
 */
public interface ReplicationMetricsMBean {

  /**
   * A system may have multiple Replication targets, each of which have a queue of files to be replicated. This returns the sum across all targets, not
   * de-duplicating files.
   *
   * @return The number of files pending replication across all targets
   */
  public int getNumFilesPendingReplication();

  /**
   * The total number of threads available to replicate data to peers. Each TabletServer has a number of threads devoted to replication, so this value is
   * affected by the number of currently active TabletServers.
   *
   * @return The number of threads available to replicate data across the instance
   */
  public int getMaxReplicationThreads();

  /**
   * Peers are systems which data can be replicated to. This is the number of peers that are defined, but this is not necessarily the number of peers which are
   * actively being replicated to.
   *
   * @return The number of peers/targets which are defined for data to be replicated to.
   */
  public int getNumConfiguredPeers();

}
