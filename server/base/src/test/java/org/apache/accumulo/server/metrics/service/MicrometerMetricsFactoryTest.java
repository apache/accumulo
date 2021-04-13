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
package org.apache.accumulo.server.metrics.service;

import java.io.IOException;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.server.ServerContext;
import org.easymock.EasyMock;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.Counter;

public class MicrometerMetricsFactoryTest {

  private static final Logger log = LoggerFactory.getLogger(MicrometerMetricsFactoryTest.class);

  @Test
  public void loadingTest() throws IOException {

    ServerContext context = EasyMock.mock(ServerContext.class);
    AccumuloConfiguration conf = EasyMock.mock(AccumuloConfiguration.class);

    EasyMock.expect(context.getConfiguration()).andReturn(conf);
    EasyMock.expect(conf.get("general.metrics.configuration.properties"))
        .andReturn(Property.GENERAL_METRICS_CONFIGURATION_PROPERTIES_FILE.getDefaultValue())
        .anyTimes();

    EasyMock.replay(context, conf);

    MicrometerMetricsFactory factory = MicrometerMetricsFactory.create(context, "test");
    log.info("Factory: {}", factory);

    Counter counter = Counter.builder("test.counter").register(factory.getRegistry());

    int count = 20;
    while (count-- > 0) {
      try {
        counter.increment(1.0);
        Thread.sleep(1_000);
      } catch (InterruptedException ex) {
        // ignore
      }
    }

  }
}
