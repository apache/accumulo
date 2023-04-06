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
package org.apache.accumulo.test.metrics;

import java.time.Duration;

import org.apache.accumulo.core.metrics.MeterRegistryFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.statsd.StatsdConfig;
import io.micrometer.statsd.StatsdFlavor;
import io.micrometer.statsd.StatsdMeterRegistry;
import io.micrometer.statsd.StatsdProtocol;

public class TestStatsDRegistryFactory implements MeterRegistryFactory {

  private static final Logger LOG = LoggerFactory.getLogger(TestStatsDRegistryFactory.class);

  public static final String SERVER_HOST = "test.meter.registry.host";
  public static final String SERVER_PORT = "test.meter.registry.port";

  @Override
  public MeterRegistry create() {

    String host = System.getProperty(SERVER_HOST, null);
    String port = System.getProperty(SERVER_PORT, null);

    if (host == null || port == null) {
      throw new IllegalArgumentException("Host and Port cannot be null");
    }

    LOG.info("host: {}, port:{}", host, port);

    StatsdConfig config = new StatsdConfig() {
      @Override
      public StatsdFlavor flavor() {
        return StatsdFlavor.DATADOG;
      }

      @Override
      public boolean enabled() {
        return true;
      }

      @Override
      public String host() {
        return host;
      }

      @Override
      public int port() {
        return Integer.parseInt(port);
      }

      @Override
      public StatsdProtocol protocol() {
        return StatsdProtocol.UDP;
      }

      @Override
      public Duration pollingFrequency() {
        return Duration.ofSeconds(3);
      }

      @Override
      public boolean buffered() {
        return false;
      }

      @Override
      public String get(String key) {
        return null;
      }
    };
    return StatsdMeterRegistry.builder(config).build();
  }

}
