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

import org.apache.accumulo.master.Master;
import org.apache.accumulo.server.metrics.Metrics;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;

public class Metrics2FateMetrics implements  Metrics, MetricsSource {

  private final Master master;
  private final MetricsSystem metricsSystem;

  public Metrics2FateMetrics(Master master, MetricsSystem metricsSystem) {
    this.master = master;
    this.metricsSystem = metricsSystem;
  }

  @Override public void register() throws Exception {

  }

  @Override public void add(String name, long time) {

  }

  @Override public boolean isEnabled() {
    return false;
  }

  @Override public void getMetrics(MetricsCollector metricsCollector, boolean b) {

  }
}
