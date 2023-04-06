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

import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.spi.balancer.data.TServerStatus;
import org.apache.accumulo.core.spi.balancer.data.TableStatistics;

public class TServerStatusImpl implements TServerStatus {
  private final TabletServerStatus thriftStatus;
  private Map<String,TableStatistics> tableInfoMap;

  public static TServerStatusImpl fromThrift(TabletServerStatus tss) {
    return (tss == null) ? null : new TServerStatusImpl(tss);
  }

  public TServerStatusImpl(TabletServerStatus thriftStatus) {
    this.thriftStatus = requireNonNull(thriftStatus);
    if (thriftStatus.getTableMap() == null) {
      tableInfoMap = null;
    } else {
      tableInfoMap = new HashMap<>();
      thriftStatus.getTableMap()
          .forEach((name, info) -> tableInfoMap.put(name, TableStatisticsImpl.fromThrift(info)));
    }
  }

  @Override
  public Map<String,TableStatistics> getTableMap() {
    return tableInfoMap;
  }

  public void setTableMap(Map<String,TableStatistics> tableInfoMap) {
    this.tableInfoMap = tableInfoMap;
  }

  @Override
  public long getLastContact() {
    return thriftStatus.getLastContact();
  }

  @Override
  public String getName() {
    return thriftStatus.getName();
  }

  @Override
  public double getOsLoad() {
    return thriftStatus.getOsLoad();
  }

  @Override
  public long getHoldTime() {
    return thriftStatus.getHoldTime();
  }

  @Override
  public long getLookups() {
    return thriftStatus.getLookups();
  }

  @Override
  public long getIndexCacheHits() {
    return thriftStatus.getIndexCacheHits();
  }

  @Override
  public long getIndexCacheRequests() {
    return thriftStatus.getIndexCacheRequest();
  }

  @Override
  public long getDataCacheHits() {
    return thriftStatus.getDataCacheHits();
  }

  @Override
  public long getDataCacheRequests() {
    return thriftStatus.getDataCacheRequest();
  }

  @Override
  public long getFlushes() {
    return thriftStatus.getFlushs();
  }

  @Override
  public long getSyncs() {
    return thriftStatus.getSyncs();
  }

  @Override
  public String getVersion() {
    return thriftStatus.getVersion();
  }

  @Override
  public long getResponseTime() {
    return thriftStatus.getResponseTime();
  }

  @Override
  public int compareTo(TServerStatus o) {
    return thriftStatus.compareTo(((TServerStatusImpl) o).thriftStatus);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TServerStatusImpl that = (TServerStatusImpl) o;
    return thriftStatus.equals(that.thriftStatus);
  }

  @Override
  public int hashCode() {
    return thriftStatus.hashCode();
  }

  @Override
  public String toString() {
    return thriftStatus.toString();
  }

  public TabletServerStatus toThrift() {
    return thriftStatus;
  }
}
