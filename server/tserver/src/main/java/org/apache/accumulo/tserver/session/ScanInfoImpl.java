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
package org.apache.accumulo.tserver.session;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.spi.scan.ScanInfo;
import org.apache.accumulo.core.util.Stat;
import org.apache.hadoop.io.Text;

import com.google.common.base.Preconditions;

public class ScanInfoImpl implements ScanInfo {

  public static class ScanMeasurer implements Runnable {

    private ScanInfoImpl scanInfo;
    private Runnable task;

    private ScanMeasurer(ScanInfoImpl scanInfo, Runnable task) {
      this.scanInfo = scanInfo;
      this.task = task;
    }

    @Override
    public void run() {
      long t1 = System.currentTimeMillis();
      task.run();
      long t2 = System.currentTimeMillis();
      scanInfo.finishedRun(t1, t2);
    }

    public ScanInfo getScanInfo() {
      return scanInfo;
    }

  }

  public static ScanMeasurer wrap(ScanInfoImpl scanInfo, Runnable r) {
    return new ScanMeasurer(scanInfo, r);
  }

  private final Type type;
  private final LongSupplier createTimeSupp;
  private long lastRunTime;
  private Stat idleStats = new Stat();
  private Stat runStats = new Stat();
  private IntSupplier numRangesSupplier;
  private int numTablets;
  private String tableId;

  public ScanInfoImpl(Type type, LongSupplier createTimeSupp, int numTablets,
      IntSupplier numRangesSupplier, String tableId) {
    super();
    this.type = type;
    this.createTimeSupp = createTimeSupp;
    this.numTablets = numTablets;
    this.numRangesSupplier = numRangesSupplier;
    this.tableId = tableId;
  }

  @Override
  public Type getScanType() {
    return type;
  }

  @Override
  public long getCreationTime() {
    return createTimeSupp.getAsLong();
  }

  @Override
  public OptionalLong getLastRunTime() {
    if (idleStats.num() == 0) {
      return OptionalLong.empty();
    } else {
      return OptionalLong.of(lastRunTime);
    }
  }

  @Override
  public Optional<Stats> getRunTimeStats() {
    return runStats.num() == 0 ? Optional.empty() : Optional.of(runStats);
  }

  @Override
  public Optional<Stats> getIdleTimeStats() {
    return idleStats.num() == 0 ? Optional.empty() : Optional.of(idleStats);
  }

  @Override
  public Stats getIdleTimeStats(long currentTime) {
    long idleTime = currentTime - getLastRunTime().orElse(getCreationTime());
    Preconditions.checkArgument(idleTime >= 0);
    Stat copy = idleStats.copy();
    copy.addStat(idleTime);
    return copy;
  }

  public void finishedRun(long start, long finish) {
    long idleTime = start - getLastRunTime().orElse(getCreationTime());
    long runTime = finish - start;
    lastRunTime = finish;
    idleStats.addStat(idleTime);
    runStats.addStat(runTime);
  }

  @Override
  public String getTableId() {
    return tableId;
  }

  @Override
  public int getNumTablets() {
    return numTablets;
  }

  @Override
  public int getNumRanges() {
    return numRangesSupplier.getAsInt();
  }

  @Override
  public Collection<Text> getFetchedFamilies() {
    throw new UnsupportedOperationException("Go away");
  }

  @Override
  public List<IteratorSetting> getScanIterators() {
    throw new UnsupportedOperationException("Go away");
  }
}
