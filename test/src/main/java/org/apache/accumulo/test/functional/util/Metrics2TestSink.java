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
package org.apache.accumulo.test.functional.util;

import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stub class that may help test metrics - if sources will publish to this as a sink, the values can
 * be captured / compared - just a stub for now. It will only be used if configured / set in the
 * hadoop-metrics2-accumulo.properties file.
 */
public class Metrics2TestSink implements MetricsSink {

  private static final Logger log = LoggerFactory.getLogger(Metrics2TestSink.class);

  @Override
  public void putMetrics(MetricsRecord metricsRecord) {
    log.info("putMetrics called {}", metricsRecord);
  }

  @Override
  public void flush() {
    log.info("flush called {}");

  }

  @Override
  public void init(SubsetConfiguration subsetConfiguration) {
    log.info("Config called {}", subsetConfiguration);
  }
}
