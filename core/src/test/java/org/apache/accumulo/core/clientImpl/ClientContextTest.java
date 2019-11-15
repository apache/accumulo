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
package org.apache.accumulo.core.clientImpl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class ClientContextTest {

  private static final String keystoreName = "/site-cfg.jceks";

  // site-cfg.jceks={'ignored.property'=>'ignored', 'instance.secret'=>'mysecret',
  // 'general.rpc.timeout'=>'timeout'}
  private static File keystore;

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN",
      justification = "provided keystoreUrl path isn't user provided")
  @BeforeClass
  public static void setUpBeforeClass() {
    URL keystoreUrl = ClientContextTest.class.getResource(keystoreName);
    assertNotNull("Could not find " + keystoreName, keystoreUrl);
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
}
