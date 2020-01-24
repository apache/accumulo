/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.master.metrics.fate;

import static org.junit.Assert.assertEquals;

import java.util.Map;
import java.util.TreeMap;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FateMetricValuesTest {

  private static final Logger log = LoggerFactory.getLogger(FateMetricValuesTest.class);

  @Test
  public void defaultValueTest() {

    FateMetricValues v = FateMetricValues.builder().build();

    assertEquals(0, v.getCurrentFateOps());
    assertEquals(0, v.getZkFateChildOpsTotal());
    assertEquals(0, v.getZkConnectionErrors());
  }

  @Test
  public void valueTest() {

    FateMetricValues.Builder builder = FateMetricValues.builder();

    FateMetricValues v =
        builder.withCurrentFateOps(1).withZkFateChildOpsTotal(2).withZkConnectionErrors(3).build();

    assertEquals(1, v.getCurrentFateOps());
    assertEquals(2, v.getZkFateChildOpsTotal());
    assertEquals(3, v.getZkConnectionErrors());
  }

  @Test
  public void foo() {
    Map<String,Long> m1 = new TreeMap<>();
    m1.put("A", 1L);
    m1.put("B", 2L);

    Map<String,Long> m2 = new TreeMap<>(m1);

    m1.put("A", 99L);
    m1.compute("A", (k, v) -> (v == null) ? 1 : v + 1);
    m1.compute("B", (k, v) -> (v == null) ? 1 : v + 1);
    m1.compute("C", (k, v) -> (v == null) ? 1 : v + 1);

    log.info("M1: {}, M2: {}", m1, m2);
  }
}
