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
package org.apache.accumulo.tserver.tablet;

import java.util.Objects;

import org.apache.accumulo.core.dataImpl.KeyExtent;

/**
 * The tablet server does periodic consistency checks to see if what is in the metadata table agrees
 * with what each tablet has in memory. When doing these checks its very important to know if the
 * tablet severs read from the metadata table overlaps in time with any tablet metadata table
 * updates. These counts allow that to be known. For example if these counts are acquired twice for
 * a tablet and are the same both times it means that no metadata table updates occurred between the
 * two acquisition times.
 */
public class MetadataUpdateCount {
  private final KeyExtent extent;
  private final long startedCount;
  private final long finishedCount;

  MetadataUpdateCount(KeyExtent extent, long startedCount, long finishedCount) {
    this.extent = Objects.requireNonNull(extent);
    this.startedCount = startedCount;
    this.finishedCount = finishedCount;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MetadataUpdateCount that = (MetadataUpdateCount) o;
    return startedCount == that.startedCount && finishedCount == that.finishedCount;
  }

  @Override
  public int hashCode() {
    return Objects.hash(startedCount, finishedCount);
  }

  public KeyExtent getExtent() {
    return extent;
  }

  /**
   * @return true if the counters were acquired while a metadata table update was being made
   */
  public boolean overlapsUpdate() {
    return startedCount != finishedCount;
  }

  public MetadataUpdateCount incrementStart() {
    return new MetadataUpdateCount(extent, startedCount + 1, finishedCount);
  }

  public MetadataUpdateCount incrementFinish() {
    return new MetadataUpdateCount(extent, startedCount, finishedCount + 1);
  }

  @Override
  public String toString() {
    return "[startedCount:" + startedCount + ",finishedCount:" + finishedCount + "]";
  }
}
