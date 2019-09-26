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
package org.apache.accumulo.test.functional;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.test.metrics.HttpMetrics2Receiver;
import org.apache.accumulo.test.metrics.HttpMetrics2SinkProperties;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Functional test that uses a test hadoop metrics 2 sink to publish metrics for verification.
 */
public class GcMetricsIT extends AccumuloClusterHarness {

  private static final Logger log = LoggerFactory.getLogger(GcMetricsIT.class);

  private Connector connector;

  private String tableName;

  private String secret;

  @Before
  public void setup() {

    connector = getConnector();

    tableName = getUniqueNames(1)[0];

    secret = cluster.getSiteConfiguration().get(Property.INSTANCE_SECRET);

  }

  @Override
  protected int defaultTimeoutSeconds() {
    return 4 * 60;
  }

  @Test
  public void gcMetricsPublished() throws IOException {

    boolean gcMetricsEnabled = cluster.getSiteConfiguration().getBoolean(Property.GC_ENABLE_METRICS2);
    boolean fateMetricsEnabled = cluster.getSiteConfiguration().getBoolean(Property.MASTER_FATE_METRICS_ENABLED);

    // test only valid when hadoop metrics 2 enabled for master and gc.
    if(!gcMetricsEnabled && !fateMetricsEnabled){
      return;
    }

    HttpMetrics2Receiver masterMetrics =
        new HttpMetrics2Receiver(HttpMetrics2SinkProperties.ACC_MASTER_SINK_PREFIX);

    HttpMetrics2Receiver gcMetrics =
        new HttpMetrics2Receiver(HttpMetrics2SinkProperties.ACC_GC_SINK_PREFIX);

    boolean haveMasterMetrics = false;
    boolean haveGcMetrics = false;

    boolean needMetricsReport = true;

    try {

      long testStart = System.currentTimeMillis();

      int count = 20;

      while ((count-- > 0) && needMetricsReport) {

        if (!haveMasterMetrics && masterMetrics.getMetrics() != null) {
          haveMasterMetrics = true;
        }

        if (!haveGcMetrics && gcMetrics.getMetrics() != null) {
          haveGcMetrics = true;
        }

        if (haveMasterMetrics && haveGcMetrics) {
          needMetricsReport = false;
        }

        log.info("waiting for metrics report. Have master: {}, Have GC: {}", haveMasterMetrics,
            haveGcMetrics);

        Thread.sleep(5_000);
      }

      assertFalse(needMetricsReport);

      assertTrue(masterMetrics.getMetrics().getTimestamp() > testStart);
      assertTrue(gcMetrics.getMetrics().getTimestamp() > testStart);

      log.debug("Master metrics: {}", masterMetrics.getMetrics());
      log.debug("GC metrics: {}", gcMetrics.getMetrics());

    } catch (Exception ex) {
      log.debug("reads", ex);
    }
  }
}
