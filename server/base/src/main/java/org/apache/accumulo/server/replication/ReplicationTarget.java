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

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Encapsulation of a cluster ID and a table ID on that cluster. Intended to ease tracking of where some data needs to be replicated.
 */
public class ReplicationTarget {

  private String clusterId, tableId;

  public ReplicationTarget(String clusterId, String tableId) {
    checkNotNull(clusterId);
    checkNotNull(tableId);

    this.clusterId = clusterId;
    this.tableId = tableId;
  }

  public String getClusterId() {
    return clusterId;
  }

  public String getTableId() {
    return tableId;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(64);
    sb.append("Remote cluster ID: ").append(clusterId).append(" remote table ID: ").append(tableId);
    return sb.toString();
  }

  @Override
  public int hashCode() {
    return clusterId.hashCode() ^ tableId.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof ReplicationTarget) {
      ReplicationTarget other = (ReplicationTarget) o;

      return clusterId.equals(other.clusterId) && tableId.equals(other.tableId);
    }

    return false;
  }

}
