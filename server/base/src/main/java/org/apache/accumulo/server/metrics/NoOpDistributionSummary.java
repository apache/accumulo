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
package org.apache.accumulo.server.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;

/**
 * Provides a default DistributionSummary that does not do anything. This can be used to prevent NPE
 * if metrics have not been initialized when a DistributionSummary is expected.
 * <p>
 * Normally DistributionSummaries are created using a builder that takes a registry.
 *
 * <pre>
 *   DistributionSummary distSum;
 *   ...
 *   public void registerMetrics(MeterRegistry registry) {
 *      ...
 *      distSum = DistributionSummary.builder("metric name").description("...").register(registry);
 *      ...
 *   }
 * </pre>
 *
 * Until the registration is called, the instance variable is null. If code then tries to update the
 * metric, a NPE is thrown. Using this class to provide a default value prevents this from occurring
 * and on registration, it is replaced with a valid instance.
 */
public class NoOpDistributionSummary implements DistributionSummary {

  private static final Logger LOG = LoggerFactory.getLogger(NoOpDistributionSummary.class);

  @Override
  public void record(double v) {
    LOG.debug("record ignored - distribution summary will not be available.");
  }

  @Override
  public long count() {
    return 0;
  }

  @Override
  public double totalAmount() {
    return 0;
  }

  @Override
  public double max() {
    return 0;
  }

  @Override
  public HistogramSnapshot takeSnapshot() {
    return new HistogramSnapshot(0L, 0.0, 0.0, null, null, null);
  }

  @Override
  public Id getId() {
    return new Id("thrift.metrics.uninitialized", Tags.of(Tag.of("none", "uninitialized")), null,
        "placeholder for uninitialized thrift metrics", Type.OTHER);
  }
}
