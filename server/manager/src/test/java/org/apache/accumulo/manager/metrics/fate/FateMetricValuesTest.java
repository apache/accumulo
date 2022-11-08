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
package org.apache.accumulo.manager.metrics.fate;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class FateMetricValuesTest {

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
}
