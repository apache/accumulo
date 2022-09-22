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
package org.apache.accumulo.core.conf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.File;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class SiteConfigurationTest {

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN",
      justification = "path to keystore not provided by user input")
  @Test
  public void testOnlySensitivePropertiesExtractedFromCredentialProvider()
      throws SecurityException {
    // site-cfg.jceks={'ignored.property'=>'ignored', 'instance.secret'=>'mysecret',
    // 'general.rpc.timeout'=>'timeout'}
    URL keystore = SiteConfigurationTest.class.getResource("/site-cfg.jceks");
    assertNotNull(keystore);
    String credProvPath = "jceks://file" + new File(keystore.getFile()).getAbsolutePath();

    var overrides =
        Map.of(Property.GENERAL_SECURITY_CREDENTIAL_PROVIDER_PATHS.getKey(), credProvPath);
    var config = SiteConfiguration.empty().withOverrides(overrides).build();

    assertEquals("mysecret", config.get(Property.INSTANCE_SECRET));
    assertNull(config.get("ignored.property"));
    assertEquals(Property.GENERAL_RPC_TIMEOUT.getDefaultValue(),
        config.get(Property.GENERAL_RPC_TIMEOUT.getKey()));
  }

  @Test
  public void testDefault() {
    var conf = SiteConfiguration.empty().build();
    assertEquals("localhost:2181", conf.get(Property.INSTANCE_ZK_HOST));
    assertEquals("DEFAULT", conf.get(Property.INSTANCE_SECRET));
    assertEquals("", conf.get(Property.INSTANCE_VOLUMES));
    assertEquals("120s", conf.get(Property.GENERAL_RPC_TIMEOUT));
    assertEquals("1G", conf.get(Property.TSERV_WAL_MAX_SIZE));
    assertEquals("org.apache.accumulo.core.spi.crypto.NoCryptoServiceFactory",
        conf.get(Property.INSTANCE_CRYPTO_FACTORY));
  }

  @Test
  public void testFile() {
    System.setProperty("DIR", "/tmp/test/dir");
    URL propsUrl = getClass().getClassLoader().getResource("accumulo2.properties");
    var conf = new SiteConfiguration.Builder().fromUrl(propsUrl).build();
    assertEquals("myhost123:2181", conf.get(Property.INSTANCE_ZK_HOST));
    assertEquals("mysecret", conf.get(Property.INSTANCE_SECRET));
    assertEquals("hdfs://localhost:8020/accumulo123", conf.get(Property.INSTANCE_VOLUMES));
    assertEquals("123s", conf.get(Property.GENERAL_RPC_TIMEOUT));
    assertEquals("256M", conf.get(Property.TSERV_WAL_MAX_SIZE));
    assertEquals("org.apache.accumulo.core.spi.crypto.PerTableCryptoServiceFactory",
        conf.get(Property.INSTANCE_CRYPTO_FACTORY));
    assertEquals(System.getenv("USER"), conf.get("general.test.user.name"));
    assertEquals("/tmp/test/dir", conf.get("general.test.user.dir"));
  }

  @Test
  public void testConfigOverrides() {
    var conf = SiteConfiguration.empty().build();
    assertEquals("localhost:2181", conf.get(Property.INSTANCE_ZK_HOST));

    conf = SiteConfiguration.empty()
        .withOverrides(Map.of(Property.INSTANCE_ZK_HOST.getKey(), "myhost:2181")).build();
    assertEquals("myhost:2181", conf.get(Property.INSTANCE_ZK_HOST));

    var results = new HashMap<String,String>();
    conf.getProperties(results, p -> p.startsWith("instance"));
    assertEquals("myhost:2181", results.get(Property.INSTANCE_ZK_HOST.getKey()));
  }
}
