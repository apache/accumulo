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
package org.apache.accumulo.tserver.session;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.IterInfo;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.core.spi.common.IteratorConfiguration;
import org.apache.accumulo.core.spi.common.Stats;
import org.apache.accumulo.core.spi.scan.ScanInfo;
import org.apache.accumulo.core.util.Stat;
import org.apache.accumulo.tserver.scan.ScanParameters;
import org.apache.accumulo.tserver.scan.ScanTask;
import org.apache.accumulo.tserver.tablet.TabletBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public abstract class ScanSession<T> extends Session implements ScanInfo {

  private static final Logger log = LoggerFactory.getLogger(ScanSession.class);

  public interface TabletResolver {
    TabletBase getTablet(KeyExtent extent);

    void close();
  }

  public static class ScanMeasurer implements Runnable {

    private final ScanSession<?> session;
    private final Runnable task;

    ScanMeasurer(ScanSession<?> session, Runnable task) {
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

  public static ScanMeasurer wrap(ScanSession<?> scanInfo, Runnable r) {
    return new ScanMeasurer(scanInfo, r);
  }

  private OptionalLong lastRunTime = OptionalLong.empty();
  private final Stat idleStats = new Stat();
  public final Stat runStats = new Stat();

  public final ScanParameters scanParams;
  private final Map<String,String> executionHints;
  private final TabletResolver tabletResolver;

  private final AtomicReference<ScanTask<T>> scanTaskRef = new AtomicReference<>();

  ScanSession(TCredentials credentials, ScanParameters scanParams,
      Map<String,String> executionHints, TabletResolver tabletResolver) {
    super(credentials);
    this.scanParams = scanParams;
    if (executionHints == null) {
      this.executionHints = Collections.emptyMap();
    } else {
      this.executionHints = Collections.unmodifiableMap(executionHints);
    }
    this.tabletResolver = tabletResolver;
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
    return Collections.unmodifiableSet(scanParams.getColumnSet());
  }

  private class IterConfImpl implements IteratorConfiguration {

    private final IterInfo ii;

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
      Map<String,String> opts = scanParams.getSsio().get(ii.iterName);
      return opts == null || opts.isEmpty() ? Collections.emptyMap()
          : Collections.unmodifiableMap(opts);
    }
  }

  @Override
  public Collection<IteratorConfiguration> getClientScanIterators() {
    return scanParams.getSsiList().stream().map(IterConfImpl::new).collect(Collectors.toList());
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

  public TabletResolver getTabletResolver() {
    return tabletResolver;
  }

  public ScanTask<T> getScanTask() {
    return scanTaskRef.get();
  }

  public void setScanTask(ScanTask<T> scanTask) {
    Objects.requireNonNull(scanTask);
    scanTaskRef.getAndUpdate(currScanTask -> {
      Preconditions.checkState(currScanTask == null,
          "Unable to set a scan task when one is already set");
      return scanTask;
    });
  }

  public void clearScanTask() {
    scanTaskRef.getAndUpdate(currScanTask -> {
      // For tracking zombie scan threads, do not want to clear the scan task if it has an active
      // thread. When the thread is not null and the task has produced a result, the thread should
      // be in
      // the process of clearing itself from the scan task.
      Preconditions.checkState(
          currScanTask == null || currScanTask.getScanThread() == null
              || currScanTask.producedResult(),
          "Can not clear scan task that is still running and has not produced a result");
      return null;
    });
  }

  private boolean loggedZombieStackTrace = false;

  public void logZombieStackTrace() {
    Preconditions.checkState(getState() == State.REMOVED);
    var scanTask = scanTaskRef.get();
    if (scanTask != null) {
      ScanTask.ScanThreadStackTrace scanStackTrace = scanTask.getStackTrace();
      if (scanStackTrace != null && !loggedZombieStackTrace) {
        var changeTimeMillis = elaspedSinceStateChange(TimeUnit.MILLISECONDS);
        var exception =
            new Exception("Fake exception to capture stack trace of zombie scan.  Thread id:"
                + scanStackTrace.threadId + " thread name:" + scanStackTrace.threadName);
        exception.setStackTrace(scanStackTrace.stackTrace);
        log.warn(
            "Scan session with no client active for {}ms has a zombie scan thread. Scan session info : {} ",
            changeTimeMillis, this, exception);
        loggedZombieStackTrace = true;
      }
    }

  }

  @Override
  public boolean cleanup() {
    tabletResolver.close();

    if (!super.cleanup()) {
      return false;
    }

    var scanTask = scanTaskRef.get();
    if (scanTask != null && scanTask.getScanThread() != null) {
      // Leave the session around if there is still a scan thread associated with it. This will
      // cause it to still show up in listscans and it will cause it to show up in the count of
      // zombie scans.
      return false;
    }

    return true;
  }

  @Override
  public String toString() {
    return super.toString() + " tableId:" + getTableId();
  }
}
