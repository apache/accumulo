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

import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.core.spi.balancer.data.TableStatistics;

public class TableStatisticsImpl implements TableStatistics {
  private final TableInfo thriftInfo;

  public static TableStatisticsImpl fromThrift(TableInfo tableInfo) {
    return tableInfo == null ? null : new TableStatisticsImpl(tableInfo);
  }

  public TableStatisticsImpl(TableInfo thriftInfo) {
    this.thriftInfo = thriftInfo;
  }

  public TableStatisticsImpl(TableStatisticsImpl other) {
    this.thriftInfo = new TableInfo(other.thriftInfo);
  }

  @Override
  public long getRecords() {
    return thriftInfo.getRecs();
  }

  @Override
  public long getRecordsInMemory() {
    return thriftInfo.getRecsInMemory();
  }

  @Override
  public int getTabletCount() {
    return thriftInfo.getTablets();
  }

  @Override
  public int getOnlineTabletCount() {
    return thriftInfo.getOnlineTablets();
  }

  public void setOnlineTabletCount(int onlineTabletCount) {
    thriftInfo.setOnlineTablets(onlineTabletCount);
  }

  @Override
  public double getIngestRate() {
    return thriftInfo.getIngestRate();
  }

  @Override
  public double getIngestByteRate() {
    return thriftInfo.getIngestByteRate();
  }

  @Override
  public double getQueryRate() {
    return thriftInfo.getQueryRate();
  }

  @Override
  public double getQueryByteRate() {
    return thriftInfo.getQueryByteRate();
  }

  @Override
  public double getScanRate() {
    return thriftInfo.getScanRate();
  }

  @Override
  public int compareTo(TableStatistics o) {
    return thriftInfo.compareTo(((TableStatisticsImpl) o).thriftInfo);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TableStatisticsImpl that = (TableStatisticsImpl) o;
    return thriftInfo.equals(that.thriftInfo);
  }

  @Override
  public int hashCode() {
    return thriftInfo.hashCode();
  }

  @Override
  public String toString() {
    return thriftInfo.toString();
  }
}
