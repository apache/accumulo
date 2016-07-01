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
package org.apache.accumulo.core.client.impl;

import java.io.File;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.CredentialProviderFactoryShim;
import org.apache.accumulo.core.conf.Property;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class ClientContextTest {

  private static boolean isCredentialProviderAvailable = false;
  private static final String keystoreName = "/site-cfg.jceks";

  // site-cfg.jceks={'ignored.property'=>'ignored', 'instance.secret'=>'mysecret', 'general.rpc.timeout'=>'timeout'}
  private static File keystore;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    try {
      Class.forName(CredentialProviderFactoryShim.HADOOP_CRED_PROVIDER_CLASS_NAME);
      isCredentialProviderAvailable = true;
    } catch (Exception e) {
      isCredentialProviderAvailable = false;
    }

    if (isCredentialProviderAvailable) {
      URL keystoreUrl = ClientContextTest.class.getResource(keystoreName);

      Assert.assertNotNull("Could not find " + keystoreName, keystoreUrl);

      keystore = new File(keystoreUrl.getFile());
    }
  }

  protected String getKeyStoreUrl(File absoluteFilePath) {
    return "jceks://file" + absoluteFilePath.getAbsolutePath();
  }

  @Test
  public void loadSensitivePropertyFromCredentialProvider() {
    if (!isCredentialProviderAvailable) {
      return;
    }

    String absPath = getKeyStoreUrl(keystore);
    ClientConfiguration clientConf = new ClientConfiguration();
    clientConf.addProperty(Property.GENERAL_SECURITY_CREDENTIAL_PROVIDER_PATHS.getKey(), absPath);

    AccumuloConfiguration accClientConf = ClientContext.convertClientConfig(clientConf);
    Assert.assertEquals("mysecret", accClientConf.get(Property.INSTANCE_SECRET));
  }

  @Test
  public void defaultValueForSensitiveProperty() {
    if (!isCredentialProviderAvailable) {
      return;
    }

    ClientConfiguration clientConf = new ClientConfiguration();

    AccumuloConfiguration accClientConf = ClientContext.convertClientConfig(clientConf);
    Assert.assertEquals(Property.INSTANCE_SECRET.getDefaultValue(), accClientConf.get(Property.INSTANCE_SECRET));
  }

  @Test
  public void sensitivePropertiesIncludedInProperties() {
    if (!isCredentialProviderAvailable) {
      return;
    }

    String absPath = getKeyStoreUrl(keystore);
    ClientConfiguration clientConf = new ClientConfiguration();
    clientConf.addProperty(Property.GENERAL_SECURITY_CREDENTIAL_PROVIDER_PATHS.getKey(), absPath);

    AccumuloConfiguration accClientConf = ClientContext.convertClientConfig(clientConf);
    Map<String,String> props = new HashMap<>();
    accClientConf.getProperties(props, x -> true);

    // Only sensitive properties are added
    Assert.assertEquals(Property.GENERAL_RPC_TIMEOUT.getDefaultValue(), props.get(Property.GENERAL_RPC_TIMEOUT.getKey()));
    // Only known properties are added
    Assert.assertFalse(props.containsKey("ignored.property"));
    Assert.assertEquals("mysecret", props.get(Property.INSTANCE_SECRET.getKey()));
  }
}
