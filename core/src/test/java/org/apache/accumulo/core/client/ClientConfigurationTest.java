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
package org.apache.accumulo.core.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.ClientConfiguration.ClientProperty;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.Test;

public class ClientConfigurationTest {
  @Test
  public void testOverrides() throws Exception {
    ClientConfiguration clientConfig = createConfig();
    assertExpectedConfig(clientConfig);
  }

  @Test
  public void testSerialization() throws Exception {
    ClientConfiguration clientConfig = createConfig();
    // sanity check that we're starting with what we're expecting
    assertExpectedConfig(clientConfig);

    String serialized = clientConfig.serialize();
    ClientConfiguration deserializedClientConfig = ClientConfiguration.deserialize(serialized);
    assertExpectedConfig(deserializedClientConfig);
  }

  private void assertExpectedConfig(ClientConfiguration clientConfig) {
    assertEquals("firstZkHosts", clientConfig.get(ClientProperty.INSTANCE_ZK_HOST));
    assertEquals("secondInstanceName", clientConfig.get(ClientProperty.INSTANCE_NAME));
    assertEquals("123s", clientConfig.get(ClientProperty.INSTANCE_ZK_TIMEOUT));
    assertEquals(ClientProperty.RPC_SSL_TRUSTSTORE_TYPE.getDefaultValue(), clientConfig.get(ClientProperty.RPC_SSL_TRUSTSTORE_TYPE));
  }

  private ClientConfiguration createConfig() {
    Configuration first = new PropertiesConfiguration();
    first.addProperty(ClientProperty.INSTANCE_ZK_HOST.getKey(), "firstZkHosts");
    Configuration second = new PropertiesConfiguration();
    second.addProperty(ClientProperty.INSTANCE_ZK_HOST.getKey(), "secondZkHosts");
    second.addProperty(ClientProperty.INSTANCE_NAME.getKey(), "secondInstanceName");
    Configuration third = new PropertiesConfiguration();
    third.addProperty(ClientProperty.INSTANCE_ZK_HOST.getKey(), "thirdZkHosts");
    third.addProperty(ClientProperty.INSTANCE_NAME.getKey(), "thirdInstanceName");
    third.addProperty(ClientProperty.INSTANCE_ZK_TIMEOUT.getKey(), "123s");
    return new ClientConfiguration(Arrays.asList(first, second, third));
  }

  @Test
  public void testConfPath() throws IOException {
    File target = new File(System.getProperty("user.dir"), "target");
    assertTrue("'target' build directory does not exist", target.exists());
    File testDir = new File(target, getClass().getName());
    if (!testDir.exists()) {
      assertTrue("Failed to create test dir " + testDir, testDir.mkdirs());
    }

    File clientConf = new File(testDir, "client.conf");
    if (!clientConf.exists()) {
      assertTrue("Failed to create file " + clientConf, clientConf.createNewFile());
    }

    // A directory should return the path with client.conf appended.
    assertEquals(clientConf.toString(), ClientConfiguration.getClientConfPath(testDir.toString()));
    // A normal file should return itself
    assertEquals(clientConf.toString(), ClientConfiguration.getClientConfPath(clientConf.toString()));

    // Something that doesn't exist should return itself (specifially, it shouldn't error)
    final File missing = new File("foobarbaz12332112");
    assertEquals(missing.toString(), ClientConfiguration.getClientConfPath(missing.toString()));

    assertNull(ClientConfiguration.getClientConfPath(null));
  }
}
