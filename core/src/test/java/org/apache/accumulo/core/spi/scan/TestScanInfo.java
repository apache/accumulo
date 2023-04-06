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
package org.apache.accumulo.core.spi.scan;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;

import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.spi.common.IteratorConfiguration;
import org.apache.accumulo.core.spi.common.Stats;
import org.apache.accumulo.core.util.Stat;

public class TestScanInfo implements ScanInfo {

  String testId;
  Type scanType;
  long creationTime;
  OptionalLong lastRunTime = OptionalLong.empty();
  Stat runTimeStats = new Stat();
  Stat idleTimeStats = new Stat();
  Map<String,String> executionHints = Collections.emptyMap();

  TestScanInfo(String testId, Type scanType, long creationTime, int... times) {
    this.testId = testId;
    this.scanType = scanType;
    this.creationTime = creationTime;

    for (int i = 0; i < times.length; i += 2) {
      long idleDuration = times[i] - (i == 0 ? 0 : times[i - 1]);
      long runDuration = times[i + 1] - times[i];
      runTimeStats.addStat(runDuration);
      idleTimeStats.addStat(idleDuration);
    }

    if (times.length > 0) {
      lastRunTime = OptionalLong.of(times[times.length - 1] + creationTime);
    }
  }

  TestScanInfo setExecutionHints(String k, String v) {
    this.executionHints = Map.of(k, v);
    return this;
  }

  @Override
  public Type getScanType() {
    return scanType;
  }

  @Override
  public TableId getTableId() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getCreationTime() {
    return creationTime;
  }

  @Override
  public OptionalLong getLastRunTime() {
    return lastRunTime;
  }

  @Override
  public Stats getRunTimeStats() {
    return runTimeStats;
  }

  @Override
  public Stats getIdleTimeStats() {
    return idleTimeStats;
  }

  @Override
  public Stats getIdleTimeStats(long currentTime) {
    Stat copy = idleTimeStats.copy();
    copy.addStat(currentTime - lastRunTime.orElse(creationTime));
    return copy;
  }

  @Override
  public Set<Column> getFetchedColumns() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Collection<IteratorConfiguration> getClientScanIterators() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<String,String> getExecutionHints() {
    return executionHints;
  }
}
