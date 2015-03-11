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
package org.apache.accumulo.server.replication;

import org.apache.accumulo.core.replication.ReplicationTarget;
import org.apache.accumulo.server.replication.proto.Replication.Status;
import org.apache.hadoop.fs.Path;

/**
 * Encapsulation of a remote system which Accumulo can replicate data to
 */
public interface ReplicaSystem {

  /**
   * Replicate the given status to the target peer
   *
   * @param p
   *          Path to the resource we're reading from
   * @param status
   *          Information to replicate
   * @param target
   *          The peer
   * @param helper
   *          Instance of ReplicaSystemHelper
   * @return A new Status for the progress that was made
   */
  public Status replicate(Path p, Status status, ReplicationTarget target, ReplicaSystemHelper helper);

  /**
   * Configure the implementation with necessary information from the system configuration
   * <p>
   * For example, we only need one implementation for Accumulo, but, for each peer, we have a ZK quorum and instance name
   */
  public void configure(String configuration);
}
