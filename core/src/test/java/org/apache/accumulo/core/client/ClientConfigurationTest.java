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
import static org.junit.Assert.assertNotEquals;

import java.io.StringReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.client.ClientConfiguration.ClientProperty;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.MapConfiguration;
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
    Configuration first = new ClientConfiguration();
    first.addProperty(ClientProperty.INSTANCE_ZK_HOST.getKey(), "firstZkHosts");
    Configuration second = new ClientConfiguration();
    second.addProperty(ClientProperty.INSTANCE_ZK_HOST.getKey(), "secondZkHosts");
    second.addProperty(ClientProperty.INSTANCE_NAME.getKey(), "secondInstanceName");
    Configuration third = new ClientConfiguration();
    third.addProperty(ClientProperty.INSTANCE_ZK_HOST.getKey(), "thirdZkHosts");
    third.addProperty(ClientProperty.INSTANCE_NAME.getKey(), "thirdInstanceName");
    third.addProperty(ClientProperty.INSTANCE_ZK_TIMEOUT.getKey(), "123s");
    return new ClientConfiguration(Arrays.asList(first, second, third));
  }

  @Test
  public void testSasl() {
    ClientConfiguration conf = new ClientConfiguration(Collections.<Configuration> emptyList());
    assertEquals("false", conf.get(ClientProperty.INSTANCE_RPC_SASL_ENABLED));
    conf.withSasl(false);
    assertEquals("false", conf.get(ClientProperty.INSTANCE_RPC_SASL_ENABLED));
    conf.withSasl(true);
    assertEquals("true", conf.get(ClientProperty.INSTANCE_RPC_SASL_ENABLED));
    final String primary = "accumulo";
    conf.withSasl(true, primary);
    assertEquals(primary, conf.get(ClientProperty.KERBEROS_SERVER_PRIMARY));
  }

  @Test
  public void testMultipleValues() throws ConfigurationException {
    String val = "comma,separated,list";

    // not the recommended way to construct a client configuration, but it works
    PropertiesConfiguration propertiesConfiguration = new PropertiesConfiguration();
    propertiesConfiguration.setListDelimiter('\0');
    propertiesConfiguration.addProperty(ClientProperty.INSTANCE_ZK_HOST.getKey(), val);
    propertiesConfiguration.load(new StringReader(ClientProperty.TRACE_SPAN_RECEIVERS.getKey() + "=" + val));
    ClientConfiguration conf = new ClientConfiguration(propertiesConfiguration);
    assertEquals(val, conf.get(ClientProperty.INSTANCE_ZK_HOST));
    assertEquals(1, conf.getList(ClientProperty.INSTANCE_ZK_HOST.getKey()).size());
    assertEquals(val, conf.get(ClientProperty.TRACE_SPAN_RECEIVERS));
    assertEquals(1, conf.getList(ClientProperty.TRACE_SPAN_RECEIVERS.getKey()).size());

    // incorrect usage that results in not being able to use multi-valued properties unless commas are escaped
    conf = new ClientConfiguration(new PropertiesConfiguration("multi-valued.client.conf"));
    assertEquals(val, conf.get(ClientProperty.INSTANCE_ZK_HOST));
    assertEquals(1, conf.getList(ClientProperty.INSTANCE_ZK_HOST.getKey()).size());
    assertNotEquals(val, conf.get(ClientProperty.TRACE_SPAN_RECEIVERS));
    assertEquals(3, conf.getList(ClientProperty.TRACE_SPAN_RECEIVERS.getKey()).size());

    // recommended usage
    conf = new ClientConfiguration("multi-valued.client.conf");
    assertEquals(val, conf.get(ClientProperty.INSTANCE_ZK_HOST));
    assertEquals(1, conf.getList(ClientProperty.INSTANCE_ZK_HOST.getKey()).size());
    assertEquals(val, conf.get(ClientProperty.TRACE_SPAN_RECEIVERS));
    assertEquals(1, conf.getList(ClientProperty.TRACE_SPAN_RECEIVERS.getKey()).size());

    // only used internally
    Map<String,String> map = new HashMap<>();
    map.put(ClientProperty.INSTANCE_ZK_HOST.getKey(), val);
    map.put(ClientProperty.TRACE_SPAN_RECEIVERS.getKey(), val);
    conf = new ClientConfiguration(new MapConfiguration(map));
    assertEquals(val, conf.get(ClientProperty.INSTANCE_ZK_HOST));
    assertEquals(1, conf.getList(ClientProperty.INSTANCE_ZK_HOST.getKey()).size());
    assertEquals(val, conf.get(ClientProperty.TRACE_SPAN_RECEIVERS));
    assertEquals(1, conf.getList(ClientProperty.TRACE_SPAN_RECEIVERS.getKey()).size());
  }

  @Test
  public void testGetAllPropertiesWithPrefix() {
    ClientConfiguration conf = new ClientConfiguration();
    conf.addProperty(ClientProperty.TRACE_SPAN_RECEIVER_PREFIX.getKey() + "first", "1st");
    conf.addProperty(ClientProperty.TRACE_SPAN_RECEIVER_PREFIX.getKey() + "second", "2nd");
    conf.addProperty("other", "value");

    Map<String,String> props = conf.getAllPropertiesWithPrefix(ClientProperty.TRACE_SPAN_RECEIVER_PREFIX);
    assertEquals(2, props.size());
    assertEquals("1st", props.get(ClientProperty.TRACE_SPAN_RECEIVER_PREFIX.getKey() + "first"));
    assertEquals("2nd", props.get(ClientProperty.TRACE_SPAN_RECEIVER_PREFIX.getKey() + "second"));
  }
}
