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
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;

import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.util.ConfigurationImpl;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

public class MetricsServiceLoaderTest {

  private static Logger log = LoggerFactory.getLogger(MetricsServiceLoaderTest.class);

  @Test
  public void loadingTest() throws IOException {
    ConfigurationCopy cc = new ConfigurationCopy(

        Map.of(Property.GENERAL_METRICS_CONFIGURATION_PROPERTIES_FILE.getKey(),
            "accumulo.metrics.configuration.properties"));

    ConfigurationImpl conf = new ConfigurationImpl(cc);

    SimpleMeterRegistry registry = new SimpleMeterRegistry();

    var propFile = Property.GENERAL_METRICS_CONFIGURATION_PROPERTIES_FILE;

    log.info("Start search {}", conf.get(propFile.getKey()));

    ClassLoader clazz = this.getClass().getClassLoader();
    InputStream in = clazz.getResourceAsStream(conf.get(propFile.getKey()));
    Properties p = new Properties();
    p.load(in);

    // log.info("Props: {}", p.entrySet());

    ServiceLoader<MetricsServiceLoader> loader = ServiceLoader.load(MetricsServiceLoader.class);
    for (MetricsServiceLoader metrics : loader) {
      log.info("register: {}", metrics.getClass().getName());
      metrics.register(registry);
    }
  }
}
