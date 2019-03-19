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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.lang.management.ManagementFactory;
import java.util.Set;
import java.util.concurrent.Future;

import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.server.metrics.MetricsSystemHelper;
import org.apache.accumulo.test.functional.util.FateUtilBase;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * IT Tests that create / run a "slow" FATE transaction to test FATE metrics, based on
 * FateConcurrentIT. This is currently a stub - need to determine how to "measure" this from the
 * metrics system and mini-cluster seems to behave differently that a "full" instance - or just have
 * not found the correct place to look.
 * <p>
 */
public class FateMetricsIT extends FateUtilBase {

  private static final Logger log = LoggerFactory.getLogger(FateMetricsIT.class);

  // when set, test will loop to allow external connection to jmx with jconsole.
  private final boolean pauseForJmxConsole = Boolean.FALSE;

  /**
   * Validate FATE metrics include the updated values.
   *
   * @throws Exception
   *           any exception is a test failure
   */
  @Test
  public void getFateMetrics() throws Exception {

    // metrics are registered in other threads - pause to allow registration to be called.
    try {
      log.debug("sleep for registration completion.");
      Thread.sleep(750);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      return;
    }

    MetricsSystem metricsSystem = MetricsSystemHelper.getInstance();

    log.debug("metrics system current config: {}", metricsSystem.currentConfig());

    // before proceeding check that base class created table and a FATE transaction (compaction)
    // is running.
    assertEquals("verify table online after created", TableState.ONLINE, getTableState(tableName));

    Future<?> compactTask = startCompactTask();

    assertTrue("compaction fate transaction exits", findFate(tableName));

    Instance instance = connector.getInstance();

    try {

      String tableId = Tables.getTableId(instance, tableName);

      log.trace("tid: {}", tableId);

      // TODO sample metrics and compare with expected?

      if (pauseForJmxConsole) {

        for (int i = 0; i < 10; i++) {

          debugMetrics(metricsSystem);

          try {
            log.info("*** Starting sleep {}", i);

            Thread.sleep(30_000);
          } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return;
          }
        }
      }
      // TODO
      // call jmx and get # fate operations;
      // long #fate = jmx.get()...
      // long numFate = 0;

      // fast check - count number of transactions
      // assertEquals(numFate, noLocks.size());

    } catch (TableNotFoundException ex) {
      throw new IllegalStateException(ex);
    }

    try {
      // test complete, cancel compaction and move on.
      connector.tableOperations().cancelCompaction(tableName);
    } catch (Exception ex) {
      log.debug("Exception thrown canceling compaction at end of test", ex);
    }

    // block if compaction still running
    compactTask.get();

  }

  private void debugMetrics(MetricsSystem metricsSystem) {

    try {

      metricsSystem.publishMetricsNow();

      MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

      Set<ObjectName> mbeans = mBeanServer.queryNames(null, null);

      for (ObjectName name : mbeans) {

        MBeanInfo info = mBeanServer.getMBeanInfo(name);
        MBeanAttributeInfo[] attrInfo = info.getAttributes();

        if (name.getDomain().startsWith("Hadoop")) {
          log.info("Hadoop: {}", name);

          log.info("Attributes for object: {}", name);
          for (MBeanAttributeInfo attr : attrInfo) {
            log.info("  {}", attr.getName());
          }

        } else {
          log.info("A:{}", name);
        }
      }
    } catch (InstanceNotFoundException | IntrospectionException | ReflectionException ex) {
      log.error("Could not retrieve metrics info", ex);
    }
  }
}
