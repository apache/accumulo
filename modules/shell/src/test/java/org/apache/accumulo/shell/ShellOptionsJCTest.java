/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.shell;

import static org.junit.Assert.assertEquals;

import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.ClientConfiguration.ClientProperty;
import org.junit.Before;
import org.junit.Test;

import com.beust.jcommander.JCommander;

public class ShellOptionsJCTest {

  ShellOptionsJC options;

  @Before
  public void setup() {
    options = new ShellOptionsJC();
  }

  @Test
  public void testSasl() throws Exception {
    JCommander jc = new JCommander();

    jc.setProgramName("accumulo shell");
    jc.addObject(options);
    jc.parse(new String[] {"--sasl"});
    ClientConfiguration clientConf = options.getClientConfiguration();
    assertEquals("true", clientConf.get(ClientProperty.INSTANCE_RPC_SASL_ENABLED));
  }

  @Test
  public void testTraceHosts() throws Exception {
    // Set the zk hosts in the client conf directly for tracing
    final String zk = "localhost:45454";
    JCommander jc = new JCommander();

    jc.setProgramName("accumulo shell");
    jc.addObject(options);
    jc.parse(new String[] {"-zh", zk});
    ClientConfiguration clientConf = options.getClientConfiguration();

    assertEquals(zk, clientConf.get(ClientProperty.INSTANCE_ZK_HOST));
  }

}
