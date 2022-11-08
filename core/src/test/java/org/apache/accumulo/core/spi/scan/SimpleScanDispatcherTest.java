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

import static org.apache.accumulo.core.spi.scan.ScanDispatch.CacheUsage.DISABLED;
import static org.apache.accumulo.core.spi.scan.ScanDispatch.CacheUsage.ENABLED;
import static org.apache.accumulo.core.spi.scan.ScanDispatch.CacheUsage.OPPORTUNISTIC;
import static org.apache.accumulo.core.spi.scan.ScanDispatch.CacheUsage.TABLE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.accumulo.core.spi.scan.ScanDispatch.CacheUsage;
import org.apache.accumulo.core.spi.scan.ScanDispatcher.DispatchParameters;
import org.apache.accumulo.core.spi.scan.ScanInfo.Type;
import org.junit.jupiter.api.Test;

public class SimpleScanDispatcherTest {
  @Test
  public void testProps() {
    assertTrue(Property.TSERV_SCAN_EXECUTORS_DEFAULT_THREADS.getKey()
        .endsWith(SimpleScanDispatcher.DEFAULT_SCAN_EXECUTOR_NAME + ".threads"));
    assertTrue(Property.TSERV_SCAN_EXECUTORS_DEFAULT_PRIORITIZER.getKey()
        .endsWith(SimpleScanDispatcher.DEFAULT_SCAN_EXECUTOR_NAME + ".prioritizer"));
  }

  private static class DispatchParametersImps implements DispatchParameters {

    private ScanInfo si;
    private Map<String,ScanExecutor> se;

    DispatchParametersImps(ScanInfo si, Map<String,ScanExecutor> se) {
      this.si = si;
      this.se = se;
    }

    @Override
    public ScanInfo getScanInfo() {
      return si;
    }

    @Override
    public Map<String,ScanExecutor> getScanExecutors() {
      return se;
    }

    @Override
    public ServiceEnvironment getServiceEnv() {
      throw new UnsupportedOperationException();
    }

  }

  private void runTest(Map<String,String> opts, Map<String,String> hints, String expectedSingle,
      String expectedMulti, CacheUsage expectedIndexCU, CacheUsage expectedDataCU) {
    TestScanInfo msi = new TestScanInfo("a", Type.MULTI, 4);
    msi.executionHints = hints;
    TestScanInfo ssi = new TestScanInfo("a", Type.SINGLE, 4);
    ssi.executionHints = hints;

    SimpleScanDispatcher ssd1 = new SimpleScanDispatcher();

    ssd1.init(new ScanDispatcher.InitParameters() {

      @Override
      public TableId getTableId() {
        throw new UnsupportedOperationException();
      }

      @Override
      public Map<String,String> getOptions() {
        return opts;
      }

      @Override
      public ServiceEnvironment getServiceEnv() {
        throw new UnsupportedOperationException();
      }
    });

    Map<String,ScanExecutor> executors = new HashMap<>();
    executors.put("E1", null);
    executors.put("E2", null);
    executors.put("E3", null);

    ScanDispatch multiPrefs = ssd1.dispatch(new DispatchParametersImps(msi, executors));
    assertEquals(expectedMulti, multiPrefs.getExecutorName());
    assertEquals(expectedIndexCU, multiPrefs.getIndexCacheUsage());
    assertEquals(expectedDataCU, multiPrefs.getDataCacheUsage());

    ScanDispatch singlePrefs = ssd1.dispatch(new DispatchParametersImps(ssi, executors));
    assertEquals(expectedSingle, singlePrefs.getExecutorName());
    assertEquals(expectedIndexCU, singlePrefs.getIndexCacheUsage());
    assertEquals(expectedDataCU, singlePrefs.getDataCacheUsage());
  }

  private void runTest(Map<String,String> opts, String expectedSingle, String expectedMulti) {
    runTest(opts, Collections.emptyMap(), expectedSingle, expectedMulti, TABLE, TABLE);
  }

  @Test
  public void testBasic() {
    String dname = SimpleScanDispatcher.DEFAULT_SCAN_EXECUTOR_NAME;

    runTest(Collections.emptyMap(), dname, dname);
    runTest(Map.of("executor", "E1"), "E1", "E1");
    runTest(Map.of("single_executor", "E2"), "E2", dname);
    runTest(Map.of("multi_executor", "E3"), dname, "E3");
    runTest(Map.of("executor", "E1", "single_executor", "E2"), "E2", "E1");
    runTest(Map.of("executor", "E1", "multi_executor", "E3"), "E1", "E3");
    runTest(Map.of("single_executor", "E2", "multi_executor", "E3"), "E2", "E3");
    runTest(Map.of("executor", "E1", "single_executor", "E2", "multi_executor", "E3"), "E2", "E3");
  }

  @Test
  public void testHints() {
    runTest(Map.of("executor", "E1"), Map.of("scan_type", "quick"), "E1", "E1", TABLE, TABLE);
    runTest(Map.of("executor", "E1", "executor.quick", "E2"), Map.of("scan_type", "quick"), "E2",
        "E2", TABLE, TABLE);
    runTest(Map.of("executor", "E1", "executor.quick", "E2", "executor.slow", "E3"),
        Map.of("scan_type", "slow"), "E3", "E3", TABLE, TABLE);
  }

  @Test
  public void testCache() {
    String dname = SimpleScanDispatcher.DEFAULT_SCAN_EXECUTOR_NAME;

    runTest(
        Map.of("executor", "E1", "cacheUsage.slow.index", "opportunistic", "cacheUsage.slow.data",
            "disabled", "cacheUsage.fast", "enabled", "executor.slow", "E2"),
        Map.of("scan_type", "slow"), "E2", "E2", OPPORTUNISTIC, DISABLED);
    runTest(
        Map.of("single_executor", "E1", "cacheUsage.slow.index", "opportunistic",
            "cacheUsage.slow.data", "disabled", "cacheUsage.fast", "enabled"),
        Map.of("scan_type", "fast"), "E1", dname, ENABLED, ENABLED);
    runTest(
        Map.of("executor", "E1", "cacheUsage.slow.index", "opportunistic", "cacheUsage.slow.data",
            "disabled", "cacheUsage.fast", "enabled"),
        Map.of("scan_type", "notconfigured"), "E1", "E1", TABLE, TABLE);
    runTest(Map.of("executor", "E1", "cacheUsage.slow.index", "opportunistic",
        "cacheUsage.slow.data", "disabled", "cacheUsage.fast", "enabled"), Map.of(), "E1", "E1",
        TABLE, TABLE);
  }
}
