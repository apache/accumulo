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

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.server.ServerContext;
import org.junit.jupiter.api.Test;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

public class MetricsInfoImplTest {
  @Test
  public void factoryTest() throws Exception {

    ServerContext context = mock(ServerContext.class);
    AccumuloConfiguration conf = mock(AccumuloConfiguration.class);
    expect(context.getConfiguration()).andReturn(conf).anyTimes();
    expect(conf.getAllPropertiesWithPrefixStripped(anyObject())).andReturn(Map.of()).anyTimes();
    expect(conf.newDeriver(anyObject())).andReturn(Map::of).anyTimes();
    replay(context, conf);
    assertNotNull(MetricsInfoImpl.getRegistryFromFactory(SPIFactory.class.getName(), context));

    assertNotNull(
        MetricsInfoImpl.getRegistryFromFactory(DeprecatedFactory.class.getName(), context));

    assertThrows(ClassNotFoundException.class,
        () -> MetricsInfoImpl.getRegistryFromFactory(String.class.getName(), context));

    verify(context, conf);
  }

  // support for org.apache.accumulo.core.metrics.MeterRegistryFactory can be removed in 3.1
  @SuppressWarnings("deprecation")
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
    public MeterRegistry create(final InitParameters params) {
      return new SimpleMeterRegistry();
    }
  }
}
