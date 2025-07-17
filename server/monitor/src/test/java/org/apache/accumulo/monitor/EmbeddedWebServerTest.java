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
package org.apache.accumulo.monitor;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.concurrent.atomic.AtomicReference;

import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.server.ServerContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Basic tests for EmbeddedWebServer
 */
public class EmbeddedWebServerTest {

  private static final AtomicReference<Monitor> monitor = new AtomicReference<>(null);

  private static final AtomicReference<ConfigurationCopy> configuration = new AtomicReference<>();

  @BeforeAll
  public static void createMocks() {

    // Mock a configuration with the new context Path
    ConfigurationCopy config = new ConfigurationCopy(DefaultConfiguration.getInstance());
    config.set(Property.MONITOR_ROOT_CONTEXT, "/test/");
    configuration.set(config);

    ServerContext contextMock = createMock(ServerContext.class);
    expect(contextMock.getConfiguration()).andReturn(config).atLeastOnce();

    Monitor monitorMock = createMock(Monitor.class);
    expect(monitorMock.getContext()).andReturn(contextMock).atLeastOnce();
    expect(monitorMock.getConfiguration()).andReturn(config).atLeastOnce();
    expect(monitorMock.getBindAddress()).andReturn("localhost:9995").atLeastOnce();

    replay(contextMock, monitorMock);
    monitor.set(monitorMock);
  }

  @AfterAll
  public static void finishMocks() {
    Monitor m = monitor.get();
    verify(m.getContext(), m);
  }

  @Test
  public void testContextPath() {
    // Test removal of trailing slash
    EmbeddedWebServer ews = new EmbeddedWebServer(monitor.get(),
        Integer.parseInt(Property.MONITOR_PORT.getDefaultValue()));
    assertEquals("/test", ews.getContextPath(),
        "Context path of " + ews.getContextPath() + " does not match");
    // Test redirect URL
    configuration.get().set(Property.MONITOR_ROOT_CONTEXT, "/../test");
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> new EmbeddedWebServer(monitor.get(),
            Integer.parseInt(Property.MONITOR_PORT.getDefaultValue())));
    assertEquals("Root context: \"/../test\" is not a valid URL", exception.getMessage());
    // Test whitespace in URL
    configuration.get().set(Property.MONITOR_ROOT_CONTEXT, "/whitespace /test");
    exception =
        assertThrows(IllegalArgumentException.class, () -> new EmbeddedWebServer(monitor.get(),
            Integer.parseInt(Property.MONITOR_PORT.getDefaultValue())));
    assertEquals("Root context: \"/whitespace /test\" is not a valid URL", exception.getMessage());
  }
}
