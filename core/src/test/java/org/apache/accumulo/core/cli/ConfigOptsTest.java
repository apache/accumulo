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
package org.apache.accumulo.core.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ConfigOptsTest {
  private ConfigOpts opts;

  @BeforeEach
  public void setUp() {
    opts = new ConfigOpts();
  }

  @Test
  public void testGetAddress() {
    opts.parseArgs(ConfigOptsTest.class.getName(),
        new String[] {"-o", Property.GENERAL_PROCESS_BIND_ADDRESS.getKey() + "=1.2.3.4"});
    assertEquals("1.2.3.4", opts.getSiteConfiguration().get(Property.GENERAL_PROCESS_BIND_ADDRESS));
  }

  @Test
  public void testGetAddress_NOne() {
    opts.parseArgs(ConfigOptsTest.class.getName(), new String[] {});
    assertEquals("0.0.0.0", opts.getSiteConfiguration().get(Property.GENERAL_PROCESS_BIND_ADDRESS));
  }

  @Test
  public void testOverrideConfig() {
    AccumuloConfiguration defaults = DefaultConfiguration.getInstance();
    assertEquals("localhost:2181", defaults.get(Property.INSTANCE_ZK_HOST));
    opts.parseArgs(ConfigOptsTest.class.getName(),
        new String[] {"-o", "instance.zookeeper.host=test:123"});
    assertEquals("test:123", opts.getSiteConfiguration().get(Property.INSTANCE_ZK_HOST));
  }

}
