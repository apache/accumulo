/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.manager;

import java.util.Objects;

import com.google.common.base.Preconditions;

/**
 * This class is used by functional services inside the manager process to determine what action
 * they should take when there are multiple active manager processes.
 */
public class PartitionData {

  /**
   * Functional services within the manager that should only run in a single manager process.
   */
  public enum SingletonManagerService {
    FATE_LOCK_CLEANUP
  }

  private final int partition;
  private final int totalInstances;

  public PartitionData(int partition, int totalInstances) {
    Preconditions.checkArgument(partition >= 0 && partition < totalInstances,
        "Incorrect partition data, partition:%s totalInstances:%s", partition, totalInstances);
    this.partition = partition;
    this.totalInstances = totalInstances;
  }

  /**
   * Each manager instance in a cluster is assigned a unique partition number in the range
   * [0,totalInstances). This method returns that unique partition number.
   */
  public int getPartition() {
    return partition;
  }

  /**
   * @return the total number of manager instances in a cluster.
   */
  public int getTotalInstances() {
    return totalInstances;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof PartitionData) {
      var opd = (PartitionData) o;
      return partition == opd.partition && totalInstances == opd.totalInstances;
    }

    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(partition, totalInstances);
  }

  @Override
  public String toString() {
    return partition + " " + totalInstances;
  }

  /**
   * Determines if a singleton manager service should run on this partition
   */
  public boolean shouldRun(SingletonManagerService service) {
    // The following should spread singleton manager services evenly over multiple manager
    // processes, rather than running them all on partition zero. Its important that all
    // partitions come to the same decision.
    return (getPartition() + service.ordinal()) % getTotalInstances() == 0;
  }
}
