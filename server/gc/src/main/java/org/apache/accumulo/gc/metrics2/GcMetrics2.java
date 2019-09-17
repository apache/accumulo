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
package org.apache.accumulo.gc.metrics2;

import java.util.concurrent.TimeUnit;

import org.apache.accumulo.gc.SimpleGarbageCollector;
import org.apache.accumulo.server.metrics.Metrics;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.impl.MsInfo;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;

public class GcMetrics2 implements Metrics, MetricsSource {

  public static final String NAME = "AccGC" + ",sub=AccGcRunStats";
  public static final String DESCRIPTION = "Accumulo GC Metrics";
  public static final String CONTEXT = "accumulo.gc";
  public static final String RECORD = "AccGc";

  private final SimpleGarbageCollector gc;
  private final MetricsSystem metricsSystem;
  private final MetricsRegistry registry;

  private static GcMetrics2 _instance;

  private MutableGaugeLong gcStarted;
  private MutableGaugeLong gcFinished;
  private MutableGaugeLong gcCandidates;
  private MutableGaugeLong gcInUse;
  private MutableGaugeLong gcDeleted;
  private MutableGaugeLong gcErrors;

  private MutableGaugeLong walStarted;
  private MutableGaugeLong walFinished;
  private MutableGaugeLong walCandidates;
  private MutableGaugeLong walInUse;
  private MutableGaugeLong walDeleted;
  private MutableGaugeLong walErrors;

  private MutableGaugeLong postOpDuration;
  private MutableGaugeLong runCycleCount;

  private GcMetrics2(final SimpleGarbageCollector gc, final MetricsSystem metricsSystem) {
    this.gc = gc;
    this.metricsSystem = metricsSystem;

    this.registry = new MetricsRegistry(Interns.info(NAME, DESCRIPTION));
    this.registry.tag(MsInfo.ProcessName, CONTEXT);

    // create gauges

    gcStarted = registry.newGauge("AccGcStarted", "gc collect start timestamp", 0L);
    gcFinished = registry.newGauge("AccGcFinished", "gc collect finished timestamp", 0L);
    gcCandidates = registry.newGauge("AccGcCandidates", "number of gc candidates", 0L);
    gcInUse = registry.newGauge("AccGcInUse", "number of gc candidates still in use", 0L);
    gcDeleted = registry.newGauge("AccGcDeleted", "number of gc candidates deleted", 0L);
    gcErrors = registry.newGauge("AccGcDeleteErrors", "number of gc delete errors", 0L);

    walStarted = registry.newGauge("AccGcWalStarted", "gc wal collect start timestamp", 0L);
    walFinished = registry.newGauge("AccGcWalFinished", "gc wal collect finished timestamp", 0L);
    walCandidates = registry.newGauge("AccGcWalCandidates", "number of gc wal candidates", 0L);
    walInUse = registry.newGauge("AccGcWalInUse", "number of candidates still inuse", 0L);
    walDeleted = registry.newGauge("AccGcWalDeleted", "number of wals deleted", 0L);
    walErrors = registry.newGauge("AccGcWalErrors", "number of wal deletion errors", 0L);

    postOpDuration = registry.newGauge("AccGcPostOpDurationMillis",
        "duration of post gc op in milliseconds", 0L);

    runCycleCount =
        registry.newGauge("AccGcRunCycleCount", "counter of completed gc collect cycles", 0L);
  }

  public static synchronized GcMetrics2 init(SimpleGarbageCollector gc,
      MetricsSystem metricsSystem) {
    if (_instance == null) {
      _instance = new GcMetrics2(gc, metricsSystem);
    }
    return _instance;
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {

    fillValues();
    // create the metrics record and publish to the registry.
    MetricsRecordBuilder builder = collector.addRecord(RECORD).setContext(CONTEXT);
    registry.snapshot(builder, all);

  }

  private void fillValues() {

    GcRunMetrics values = gc.getGcRunMetrics();

    gcStarted.set(values.getLastCollect().started);
    gcFinished.set(values.getLastCollect().finished);
    gcCandidates.set(values.getLastCollect().candidates);
    gcInUse.set(values.getLastCollect().inUse);
    gcDeleted.set(values.getLastCollect().deleted);
    gcErrors.set(values.getLastCollect().errors);

    walStarted.set(values.getLastWalCollect().started);
    walFinished.set(values.getLastWalCollect().finished);
    walCandidates.set(values.getLastWalCollect().candidates);
    walInUse.set(values.getLastWalCollect().inUse);
    walDeleted.set(values.getLastWalCollect().deleted);
    walErrors.set(values.getLastWalCollect().errors);

    postOpDuration.set(TimeUnit.NANOSECONDS.toMillis(values.getPostOpDuration()));
    runCycleCount.set(values.getRunCycleCount());
  }

  @Override
  public void register() {
    metricsSystem.register(NAME, DESCRIPTION, this);
  }

  @Override
  public void add(String name, long time) {
    throw new UnsupportedOperationException("add() is not implemented");
  }

  @Override
  public boolean isEnabled() {
    return true;
  }
}
