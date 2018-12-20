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
package org.apache.accumulo.core.conf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.File;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class SiteConfigurationTest {
  private static boolean isCredentialProviderAvailable;

  @BeforeClass
  public static void checkCredentialProviderAvailable() {
    try {
      Class.forName(CredentialProviderFactoryShim.HADOOP_CRED_PROVIDER_CLASS_NAME);
      isCredentialProviderAvailable = true;
    } catch (Exception e) {
      isCredentialProviderAvailable = false;
    }
  }

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN",
      justification = "path to keystore not provided by user input")
  @Test
  public void testOnlySensitivePropertiesExtractedFromCredentialProvider()
      throws SecurityException {
    if (!isCredentialProviderAvailable) {
      return;
    }

    // site-cfg.jceks={'ignored.property'=>'ignored', 'instance.secret'=>'mysecret',
    // 'general.rpc.timeout'=>'timeout'}
    URL keystore = SiteConfigurationTest.class.getResource("/site-cfg.jceks");
    assertNotNull(keystore);
    String credProvPath = "jceks://file" + new File(keystore.getFile()).getAbsolutePath();

    SiteConfiguration config = new SiteConfiguration(ImmutableMap
        .of(Property.GENERAL_SECURITY_CREDENTIAL_PROVIDER_PATHS.getKey(), credProvPath));

    assertEquals("mysecret", config.get(Property.INSTANCE_SECRET));
    assertNull(config.get("ignored.property"));
    assertEquals(Property.GENERAL_RPC_TIMEOUT.getDefaultValue(),
        config.get(Property.GENERAL_RPC_TIMEOUT.getKey()));
  }

  @Test
  public void testDefault() {
    SiteConfiguration conf = new SiteConfiguration();
    assertEquals("localhost:2181", conf.get(Property.INSTANCE_ZK_HOST));
    assertEquals("DEFAULT", conf.get(Property.INSTANCE_SECRET));
    assertEquals("", conf.get(Property.INSTANCE_VOLUMES));
    assertEquals("120s", conf.get(Property.GENERAL_RPC_TIMEOUT));
    assertEquals("1g", conf.get(Property.TSERV_WALOG_MAX_SIZE));
    assertEquals("org.apache.accumulo.core.cryptoImpl.NoCryptoService",
        conf.get(Property.INSTANCE_CRYPTO_SERVICE));
  }

  @Test
  public void testFile() {
    URL propsUrl = getClass().getClassLoader().getResource("accumulo2.properties");
    SiteConfiguration conf = new SiteConfiguration(propsUrl);
    assertEquals("myhost123:2181", conf.get(Property.INSTANCE_ZK_HOST));
    assertEquals("mysecret", conf.get(Property.INSTANCE_SECRET));
    assertEquals("hdfs://localhost:8020/accumulo123", conf.get(Property.INSTANCE_VOLUMES));
    assertEquals("123s", conf.get(Property.GENERAL_RPC_TIMEOUT));
    assertEquals("256M", conf.get(Property.TSERV_WALOG_MAX_SIZE));
    assertEquals("org.apache.accumulo.core.cryptoImpl.AESCryptoService",
        conf.get(Property.INSTANCE_CRYPTO_SERVICE));
  }

  @Test
  public void testConfigOverrides() {
    SiteConfiguration conf = new SiteConfiguration();
    assertEquals("localhost:2181", conf.get(Property.INSTANCE_ZK_HOST));

    conf = new SiteConfiguration((URL) null,
        ImmutableMap.of(Property.INSTANCE_ZK_HOST.getKey(), "myhost:2181"));
    assertEquals("myhost:2181", conf.get(Property.INSTANCE_ZK_HOST));

    Map<String,String> results = new HashMap<>();
    conf.getProperties(results, p -> p.startsWith("instance"));
    assertEquals("myhost:2181", results.get(Property.INSTANCE_ZK_HOST.getKey()));
  }
}
