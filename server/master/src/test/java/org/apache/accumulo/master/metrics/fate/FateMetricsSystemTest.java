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

import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.easymock.EasyMock;
import org.junit.Test;

public class FateMetricsSystemTest {

  public static MetricsSystem mockMetricsSystem() {
    MetricsSystem ms = EasyMock.createMock(MetricsSystem.class);
    DefaultMetricsSystem.setInstance(ms);
    return ms;
  }

  @Test
  public void x() {

    MetricsRecordBuilder rb = EasyMock.createMock(MetricsRecordBuilder.class);
    MetricsCollector mc = rb.parent();

    MetricsSystem ms = mockMetricsSystem();

    // MetricsSource s = ms.getSource("all");
    // s.getMetrics(mc, true);

    ms.publishMetricsNow();
  }
}
