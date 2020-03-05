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
package org.apache.accumulo.master.metrics.fate;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class FateMetricSnapshotTest {

  @Test
  public void defaultValueTest() {

    FateMetricSnapshot v = FateMetricSnapshot.builder().build();

    assertEquals(0, v.getCurrentFateOps());
    assertEquals(0, v.getZkFateChildOpsTotal());
    assertEquals(0, v.getZkConnectionErrors());
  }

  @Test
  public void valueTest() {

    FateMetricSnapshot.Builder builder = FateMetricSnapshot.builder();

    FateMetricSnapshot v =
        builder.withCurrentFateOps(1).withZkFateChildOpsTotal(2).withZkConnectionErrors(3).build();

    assertEquals(1, v.getCurrentFateOps());
    assertEquals(2, v.getZkFateChildOpsTotal());
    assertEquals(3, v.getZkConnectionErrors());

    FateMetricSnapshot.Builder builder2 = builder.copy(v);

    FateMetricSnapshot v2 = builder2.withCurrentFateOps(11).build();

    assertEquals(11, v2.getCurrentFateOps());
    assertEquals(2, v2.getZkFateChildOpsTotal());
    assertEquals(3, v2.getZkConnectionErrors());

    v2 = builder2.withZkFateChildOpsTotal(22).build();

    assertEquals(11, v2.getCurrentFateOps());
    assertEquals(22, v2.getZkFateChildOpsTotal());
    assertEquals(3, v2.getZkConnectionErrors());

    v2 = builder2.withZkConnectionErrors(33).build();

    assertEquals(11, v2.getCurrentFateOps());
    assertEquals(22, v2.getZkFateChildOpsTotal());
    assertEquals(33, v2.getZkConnectionErrors());

    v2 = builder2.incrZkConnectionErrors().build();

    assertEquals(11, v2.getCurrentFateOps());
    assertEquals(22, v2.getZkFateChildOpsTotal());
    assertEquals(34, v2.getZkConnectionErrors());
  }
}
