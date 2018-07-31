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
package org.apache.accumulo.core.spi.scan;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.spi.scan.ScanInfo.Type;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class SimpleScanDispatcherTest {
  @Test
  public void testProps() {
    Assert.assertTrue(Property.TSERV_SCAN_EXECUTORS_DEFAULT_THREADS.getKey()
        .endsWith(SimpleScanDispatcher.DEFAULT_SCAN_EXECUTOR_NAME + ".threads"));
    Assert.assertTrue(Property.TSERV_SCAN_EXECUTORS_DEFAULT_PRIORITIZER.getKey()
        .endsWith(SimpleScanDispatcher.DEFAULT_SCAN_EXECUTOR_NAME + ".prioritizer"));
  }

  private void runTest(Map<String,String> opts, Map<String,String> hints, String expectedSingle,
      String expectedMulti) {
    TestScanInfo msi = new TestScanInfo("a", Type.MULTI, 4);
    msi.executionHints = hints;
    TestScanInfo ssi = new TestScanInfo("a", Type.SINGLE, 4);
    ssi.executionHints = hints;

    SimpleScanDispatcher ssd1 = new SimpleScanDispatcher();
    ssd1.init(opts);

    Map<String,ScanExecutor> executors = new HashMap<>();
    executors.put("E1", null);
    executors.put("E2", null);
    executors.put("E3", null);

    Assert.assertEquals(expectedMulti, ssd1.dispatch(msi, executors));
    Assert.assertEquals(expectedSingle, ssd1.dispatch(ssi, executors));
  }

  private void runTest(Map<String,String> opts, String expectedSingle, String expectedMulti) {
    runTest(opts, Collections.emptyMap(), expectedSingle, expectedMulti);
  }

  @Test
  public void testBasic() {
    String dname = SimpleScanDispatcher.DEFAULT_SCAN_EXECUTOR_NAME;

    runTest(Collections.emptyMap(), dname, dname);
    runTest(ImmutableMap.of("executor", "E1"), "E1", "E1");
    runTest(ImmutableMap.of("single_executor", "E2"), "E2", dname);
    runTest(ImmutableMap.of("multi_executor", "E3"), dname, "E3");
    runTest(ImmutableMap.of("executor", "E1", "single_executor", "E2"), "E2", "E1");
    runTest(ImmutableMap.of("executor", "E1", "multi_executor", "E3"), "E1", "E3");
    runTest(ImmutableMap.of("single_executor", "E2", "multi_executor", "E3"), "E2", "E3");
    runTest(ImmutableMap.of("executor", "E1", "single_executor", "E2", "multi_executor", "E3"),
        "E2", "E3");
  }

  @Test
  public void testHints() {
    runTest(ImmutableMap.of("executor", "E1", "heed_hints", "true"), ImmutableMap.of(), "E1", "E1");
    runTest(ImmutableMap.of("executor", "E1", "heed_hints", "true"),
        ImmutableMap.of("executor", "E2"), "E2", "E2");
    runTest(ImmutableMap.of("executor", "E1", "heed_hints", "true"),
        ImmutableMap.of("executor", "E5"), "E1", "E1");
    runTest(ImmutableMap.of("executor", "E1", "heed_hints", "true", "ignored_hint_action", "fail"),
        ImmutableMap.of("executor", "E5"), "E1", "E1");
    runTest(ImmutableMap.of("executor", "E1", "heed_hints", "true", "bad_hint_action", "fail",
        "ignored_hint_action", "fail"), ImmutableMap.of("executor", "E2"), "E2", "E2");
    runTest(ImmutableMap.of("executor", "E1", "heed_hints", "false"),
        ImmutableMap.of("executor", "E2"), "E1", "E1");
    runTest(ImmutableMap.of("executor", "E1"), ImmutableMap.of("executor", "E2"), "E1", "E1");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadHint() {
    runTest(ImmutableMap.of("executor", "E1", "heed_hints", "true", "bad_hint_action", "fail"),
        ImmutableMap.of("executor", "E5"), "E2", "E2");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testIgnoredHint() {
    runTest(ImmutableMap.of("executor", "E1", "heed_hints", "false", "ignored_hint_action", "fail"),
        ImmutableMap.of("executor", "E2"), "E1", "E1");
  }
}
