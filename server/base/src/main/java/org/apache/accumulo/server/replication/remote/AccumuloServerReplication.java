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
package org.apache.accumulo.server.replication.remote;

import org.apache.accumulo.core.replication.ReplicationTarget;
import org.apache.accumulo.core.replication.ServerReplication;
import org.apache.hadoop.fs.Path;

/**
 * Implementation to replicate the given data to another Accumulo instance
 */
public class AccumuloServerReplication implements ServerReplication {

  /**
   * Replicate the data given by the supplied {@code localFile} and delimited by the provided
   * {@code offset} and {@code count} (number of records).
   */
  @Override
  public boolean replicate(ReplicationTarget target, Path localFile, long offset, long count) {
    // TODO Auto-generated method stub
    return false;
  }

  /**
   * @return The ZooKeeper connection string for the remote instance
   */
  public String getRemoteZooKeepers() {
    return null;
  }

  /**
   * @return The Accumulo instance name for the remote instance
   */
  public String getRemoteInstanceName() {
    return null;
  }
}
