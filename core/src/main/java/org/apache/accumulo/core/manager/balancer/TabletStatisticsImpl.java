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
package org.apache.accumulo.core.manager.balancer;

import static java.util.Objects.requireNonNull;

import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.TabletIdImpl;
import org.apache.accumulo.core.spi.balancer.data.TabletStatistics;
import org.apache.accumulo.core.tabletserver.thrift.TabletStats;

public class TabletStatisticsImpl implements TabletStatistics {
  private final TabletStats thriftStats;
  private final TabletId tabletId;

  public TabletStatisticsImpl(TabletStats thriftStats) {
    this.thriftStats = requireNonNull(thriftStats);
    tabletId = new TabletIdImpl(KeyExtent.fromThrift(thriftStats.getExtent()));
  }

  @Override
  public TabletId getTabletId() {
    return tabletId;
  }

  @Override
  public long getNumEntries() {
    return thriftStats.getNumEntries();
  }

  @Override
  public long getSplitCreationTime() {
    return thriftStats.getSplitCreationTime();
  }

  @Override
  public double getIngestRate() {
    return thriftStats.getIngestRate();
  }

  @Override
  public double getQueryRate() {
    return thriftStats.getQueryRate();
  }

  @Override
  public int compareTo(TabletStatistics o) {
    return thriftStats.compareTo(((TabletStatisticsImpl) o).thriftStats);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TabletStatisticsImpl that = (TabletStatisticsImpl) o;
    return thriftStats.equals(that.thriftStats);
  }

  @Override
  public int hashCode() {
    return thriftStats.hashCode();
  }

  @Override
  public String toString() {
    return thriftStats.toString();
  }
}
