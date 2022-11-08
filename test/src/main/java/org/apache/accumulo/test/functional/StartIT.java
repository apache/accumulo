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
package org.apache.accumulo.test.functional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.time.Duration;

import org.apache.accumulo.cluster.ClusterControl;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.start.TestMain;
import org.junit.jupiter.api.Test;

public class StartIT extends AccumuloClusterHarness {

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofSeconds(30);
  }

  @Test
  public void test() throws Exception {
    ClusterControl control = getCluster().getClusterControl();

    assertNotEquals(0, control.exec(TestMain.class, new String[] {"exception"}));
    assertEquals(0, control.exec(TestMain.class, new String[] {"success"}));
    assertNotEquals(0, control.exec(TestMain.class, new String[0]));
  }

}
