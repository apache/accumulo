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
package org.apache.accumulo.core.metrics;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

public class MetricsUtilTest {
  @Test
  public void factoryTest() throws Exception {

    assertNotNull(MetricsUtil.getRegistryFromFactory(SPIFactory.class.getName()));

    assertNotNull(MetricsUtil.getRegistryFromFactory(DeprecatedFactory.class.getName()));

    assertThrows(ClassNotFoundException.class,
        () -> MetricsUtil.getRegistryFromFactory(String.class.getName()));
  }

  @SuppressWarnings({"deprecation",
      "support for org.apache.accumulo.core.metrics.MeterRegistryFactory can be removed in 3.1"})
  static final class DeprecatedFactory
      implements org.apache.accumulo.core.metrics.MeterRegistryFactory {
    DeprecatedFactory() {

    }

    @Override
    public MeterRegistry create() {
      return new SimpleMeterRegistry();
    }
  }

  static class SPIFactory implements org.apache.accumulo.core.spi.metrics.MeterRegistryFactory {

    SPIFactory() {

    }

    @Override
    public MeterRegistry create() {
      return new SimpleMeterRegistry();
    }
  }
}
