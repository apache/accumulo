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
package org.apache.accumulo.core.clientImpl;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.File;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ConditionalWriterConfig;
import org.apache.accumulo.core.client.Durability;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.conf.Property;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class ClientContextTest {

  private static final String keystoreName = "/site-cfg.jceks";

  // site-cfg.jceks={'ignored.property'=>'ignored', 'instance.secret'=>'mysecret',
  // 'general.rpc.timeout'=>'timeout'}
  private static File keystore;

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN",
      justification = "provided keystoreUrl path isn't user provided")
  @BeforeAll
  public static void setUpBeforeAll() {
    URL keystoreUrl = ClientContextTest.class.getResource(keystoreName);
    assertNotNull(keystoreUrl, "Could not find " + keystoreName);
    keystore = new File(keystoreUrl.getFile());
  }

  protected String getKeyStoreUrl(File absoluteFilePath) {
    return "jceks://file" + absoluteFilePath.getAbsolutePath();
  }

  @Test
  public void loadSensitivePropertyFromCredentialProvider() {
    String absPath = getKeyStoreUrl(keystore);
    Properties props = new Properties();
    props.setProperty(Property.GENERAL_SECURITY_CREDENTIAL_PROVIDER_PATHS.getKey(), absPath);
    AccumuloConfiguration accClientConf = ClientConfConverter.toAccumuloConf(props);
    assertEquals("mysecret", accClientConf.get(Property.INSTANCE_SECRET));
  }

  @Test
  public void defaultValueForSensitiveProperty() {
    Properties props = new Properties();
    AccumuloConfiguration accClientConf = ClientConfConverter.toAccumuloConf(props);
    assertEquals(Property.INSTANCE_SECRET.getDefaultValue(),
        accClientConf.get(Property.INSTANCE_SECRET));
  }

  @Test
  public void sensitivePropertiesIncludedInProperties() {
    String absPath = getKeyStoreUrl(keystore);
    Properties clientProps = new Properties();
    clientProps.setProperty(Property.GENERAL_SECURITY_CREDENTIAL_PROVIDER_PATHS.getKey(), absPath);

    AccumuloConfiguration accClientConf = ClientConfConverter.toAccumuloConf(clientProps);
    Map<String,String> props = new HashMap<>();
    accClientConf.getProperties(props, x -> true);

    // Only sensitive properties are added
    assertEquals(Property.GENERAL_RPC_TIMEOUT.getDefaultValue(),
        props.get(Property.GENERAL_RPC_TIMEOUT.getKey()));
    // Only known properties are added
    assertFalse(props.containsKey("ignored.property"));
    assertEquals("mysecret", props.get(Property.INSTANCE_SECRET.getKey()));
  }

  @Test
  public void testGetBatchWriterConfigUsingDefaults() {
    Properties props = new Properties();
    BatchWriterConfig batchWriterConfig = ClientContext.getBatchWriterConfig(props);
    assertNotNull(batchWriterConfig);

    long expectedMemory = ConfigurationTypeHelper
        .getMemoryAsBytes(ClientProperty.BATCH_WRITER_MEMORY_MAX.getDefaultValue());
    assertEquals(expectedMemory, batchWriterConfig.getMaxMemory());

    // If the value of BATCH_WRITE_LATENCY_MAX or BATCH_WRITER_TIMEOUT_MAX, is set to zero,
    // Long.MAX_VALUE is returned. Effectively, this will cause data to be held in memory
    // indefinitely for BATCH_WRITE_LATENCY_MAX and for no timeout, for BATCH_WRITER_TIMEOUT_MAX.

    long expectedLatency = ConfigurationTypeHelper
        .getTimeInMillis(ClientProperty.BATCH_WRITER_LATENCY_MAX.getDefaultValue());

    // default latency should be 120000 ms
    assertEquals(120000L, expectedLatency);
    assertEquals(expectedLatency, batchWriterConfig.getMaxLatency(MILLISECONDS));

    long expectedTimeout = ConfigurationTypeHelper
        .getTimeInMillis(ClientProperty.BATCH_WRITER_TIMEOUT_MAX.getDefaultValue());
    if (expectedTimeout == 0) {
      expectedTimeout = Long.MAX_VALUE;
    }
    assertEquals(expectedTimeout, Long.MAX_VALUE);
    assertEquals(expectedTimeout, batchWriterConfig.getTimeout(MILLISECONDS));

    int expectedThreads =
        Integer.parseInt(ClientProperty.BATCH_WRITER_THREADS_MAX.getDefaultValue());
    assertEquals(expectedThreads, batchWriterConfig.getMaxWriteThreads());

    Durability expectedDurability =
        Durability.valueOf(ClientProperty.BATCH_WRITER_DURABILITY.getDefaultValue().toUpperCase());
    assertEquals(expectedDurability, batchWriterConfig.getDurability());
  }

  @Test
  public void testGetBatchWriterConfigNotUsingDefaults() {
    Properties props = new Properties();

    // set properties to non-default values
    props.setProperty(ClientProperty.BATCH_WRITER_MEMORY_MAX.getKey(), "10M");
    props.setProperty(ClientProperty.BATCH_WRITER_LATENCY_MAX.getKey(), "40");
    props.setProperty(ClientProperty.BATCH_WRITER_TIMEOUT_MAX.getKey(), "15");
    props.setProperty(ClientProperty.BATCH_WRITER_THREADS_MAX.getKey(), "12");
    props.setProperty(ClientProperty.BATCH_WRITER_DURABILITY.getKey(), Durability.FLUSH.name());

    BatchWriterConfig batchWriterConfig = ClientContext.getBatchWriterConfig(props);
    assertNotNull(batchWriterConfig);

    long expectedMemory = ConfigurationTypeHelper
        .getMemoryAsBytes(ClientProperty.BATCH_WRITER_MEMORY_MAX.getValue(props));
    assertEquals(expectedMemory, batchWriterConfig.getMaxMemory());

    assertEquals(40, batchWriterConfig.getMaxLatency(SECONDS));
    assertEquals(40000, batchWriterConfig.getMaxLatency(MILLISECONDS));

    assertEquals(15, batchWriterConfig.getTimeout(SECONDS));
    assertEquals(15000, batchWriterConfig.getTimeout(MILLISECONDS));

    long expectedThreads = ClientProperty.BATCH_WRITER_THREADS_MAX.getInteger(props);
    assertEquals(expectedThreads, batchWriterConfig.getMaxWriteThreads());

    Durability expectedDurability =
        Durability.valueOf(ClientProperty.BATCH_WRITER_DURABILITY.getValue(props).toUpperCase());
    assertEquals(expectedDurability, batchWriterConfig.getDurability());
  }

  @Test
  public void testGetConditionalWriterConfigUsingDefaults() {
    Properties props = new Properties();
    ConditionalWriterConfig conditionalWriterConfig =
        ClientContext.getConditionalWriterConfig(props);
    assertNotNull(conditionalWriterConfig);

    // If the value of CONDITIONAL_WRITER_TIMEOUT_MAX is set to zero, Long.MAX_VALUE is returned.
    // Effectively, this indicates there is no timeout for CONDITIONAL_WRITER_TIMEOUT_MAX
    long expectedTimeout = ConfigurationTypeHelper
        .getTimeInMillis(ClientProperty.CONDITIONAL_WRITER_TIMEOUT_MAX.getDefaultValue());
    if (expectedTimeout == 0) {
      expectedTimeout = Long.MAX_VALUE;
    }
    assertEquals(expectedTimeout, Long.MAX_VALUE);
    assertEquals(expectedTimeout, conditionalWriterConfig.getTimeout(MILLISECONDS));

    int expectedThreads =
        Integer.parseInt(ClientProperty.CONDITIONAL_WRITER_THREADS_MAX.getDefaultValue());
    assertEquals(expectedThreads, conditionalWriterConfig.getMaxWriteThreads());

    Durability expectedDurability = Durability
        .valueOf(ClientProperty.CONDITIONAL_WRITER_DURABILITY.getDefaultValue().toUpperCase());
    assertEquals(expectedDurability, conditionalWriterConfig.getDurability());
  }

  @Test
  public void testGetConditionalWriterConfigNotUsingDefaults() {
    Properties props = new Properties();

    // set properties to non-default values
    props.setProperty(ClientProperty.CONDITIONAL_WRITER_TIMEOUT_MAX.getKey(), "17");
    props.setProperty(ClientProperty.CONDITIONAL_WRITER_THREADS_MAX.getKey(), "14");
    props.setProperty(ClientProperty.CONDITIONAL_WRITER_DURABILITY.getKey(),
        Durability.SYNC.name());

    ConditionalWriterConfig conditionalWriterConfig =
        ClientContext.getConditionalWriterConfig(props);
    assertNotNull(conditionalWriterConfig);

    assertEquals(17, conditionalWriterConfig.getTimeout(SECONDS));
    assertEquals(17000, conditionalWriterConfig.getTimeout(MILLISECONDS));

    long expectedThreads = ClientProperty.CONDITIONAL_WRITER_THREADS_MAX.getInteger(props);
    assertEquals(expectedThreads, conditionalWriterConfig.getMaxWriteThreads());

    Durability expectedDurability = Durability
        .valueOf(ClientProperty.CONDITIONAL_WRITER_DURABILITY.getValue(props).toUpperCase());
    assertEquals(expectedDurability, conditionalWriterConfig.getDurability());
  }

}
