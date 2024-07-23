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
package org.apache.accumulo.core.spi.metrics;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.config.MeterFilter;

public class FilteringLoggingMRFactory extends LoggingMeterRegistryFactory {

  // See https://docs.micrometer.io/micrometer/reference/concepts/meter-filters.html
  public MeterRegistry filteredCreate(final InitParameters params, String... filter) {
    var meterRegistry = super.create(params);
    MeterFilter myFilter = MeterFilter.deny(createIdPredicate(filter));
    meterRegistry.config().meterFilter(myFilter);
    return meterRegistry;
  }

  public static Predicate<Meter.Id> createIdPredicate(String... strings) {
    Set<String> stringSet = new HashSet<>(Arrays.asList(strings));

    return id -> stringSet.contains(id.getName());
  }
}
