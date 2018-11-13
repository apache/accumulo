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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;

import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.dataImpl.thrift.IterInfo;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.core.spi.common.IteratorConfiguration;
import org.apache.accumulo.core.spi.common.Stats;
import org.apache.accumulo.core.spi.scan.ScanInfo;
import org.apache.accumulo.core.util.Stat;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public abstract class ScanSession extends Session implements ScanInfo {

  public static class ScanMeasurer implements Runnable {

    private ScanSession session;
    private Runnable task;

    ScanMeasurer(ScanSession session, Runnable task) {
      this.session = session;
      this.task = task;
    }

    @Override
    public void run() {
      long t1 = System.currentTimeMillis();
      task.run();
      long t2 = System.currentTimeMillis();
      session.finishedRun(t1, t2);
    }

    public ScanInfo getScanInfo() {
      return session;
    }
  }

  public static ScanMeasurer wrap(ScanSession scanInfo, Runnable r) {
    return new ScanMeasurer(scanInfo, r);
  }

  private OptionalLong lastRunTime = OptionalLong.empty();
  private Stat idleStats = new Stat();
  public Stat runStats = new Stat();

  public final HashSet<Column> columnSet;
  public final List<IterInfo> ssiList;
  public final Map<String,Map<String,String>> ssio;
  public final Authorizations auths;
  private Map<String,String> executionHints;

  ScanSession(TCredentials credentials, HashSet<Column> cols, List<IterInfo> ssiList,
      Map<String,Map<String,String>> ssio, Authorizations auths,
      Map<String,String> executionHints) {
    super(credentials);
    this.columnSet = cols;
    this.ssiList = ssiList;
    this.ssio = ssio;
    this.auths = auths;
    if (executionHints == null) {
      this.executionHints = Collections.emptyMap();
    } else {
      this.executionHints = Collections.unmodifiableMap(executionHints);
    }
  }

  @Override
  public long getCreationTime() {
    return startTime;
  }

  @Override
  public OptionalLong getLastRunTime() {
    return lastRunTime;
  }

  @Override
  public Stats getRunTimeStats() {
    return runStats;
  }

  @Override
  public Stats getIdleTimeStats() {
    return idleStats;
  }

  @Override
  public Stats getIdleTimeStats(long currentTime) {
    long idleTime = currentTime - getLastRunTime().orElse(getCreationTime());
    Preconditions.checkArgument(idleTime >= 0);
    Stat copy = idleStats.copy();
    copy.addStat(idleTime);
    return copy;
  }

  @Override
  public Set<Column> getFetchedColumns() {
    return Collections.unmodifiableSet(columnSet);
  }

  private class IterConfImpl implements IteratorConfiguration {

    private IterInfo ii;

    IterConfImpl(IterInfo ii) {
      this.ii = ii;
    }

    @Override
    public String getIteratorClass() {
      return ii.className;
    }

    @Override
    public String getName() {
      return ii.iterName;
    }

    @Override
    public int getPriority() {
      return ii.priority;
    }

    @Override
    public Map<String,String> getOptions() {
      Map<String,String> opts = ssio.get(ii.iterName);
      return opts == null || opts.isEmpty() ? Collections.emptyMap()
          : Collections.unmodifiableMap(opts);
    }
  }

  @Override
  public Collection<IteratorConfiguration> getClientScanIterators() {
    return Lists.transform(ssiList, IterConfImpl::new);
  }

  @Override
  public Map<String,String> getExecutionHints() {
    return executionHints;
  }

  public void finishedRun(long start, long finish) {
    long idleTime = start - getLastRunTime().orElse(getCreationTime());
    long runTime = finish - start;
    lastRunTime = OptionalLong.of(finish);
    idleStats.addStat(idleTime);
    runStats.addStat(runTime);
  }
}
