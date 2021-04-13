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
import org.easymock.EasyMock;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

public class MetricsServiceFactoryTest {

  private static final Logger log = LoggerFactory.getLogger(MetricsServiceFactoryTest.class);

  @Test
  public void loadingTest() throws IOException {

    AccumuloConfiguration conf = EasyMock.mock(AccumuloConfiguration.class);
    EasyMock.expect(conf.get("general.metrics.configuration.properties"))
        .andReturn(Property.GENERAL_METRICS_CONFIGURATION_PROPERTIES_FILE.getDefaultValue())
        .anyTimes();

    EasyMock.replay(conf);

    SimpleMeterRegistry registry = new SimpleMeterRegistry();

    MetricsServiceFactory factory = new MetricsServiceFactory(conf, registry);

    log.info("Factory: {}", factory);

  }
}
