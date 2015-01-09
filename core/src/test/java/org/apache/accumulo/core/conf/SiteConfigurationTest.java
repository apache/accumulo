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

import java.io.File;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.conf.AccumuloConfiguration.AllFilter;
import org.apache.hadoop.conf.Configuration;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

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

  @Test
  public void testOnlySensitivePropertiesExtractedFromCredetialProvider() throws SecurityException, NoSuchMethodException {
    if (!isCredentialProviderAvailable) {
      return;
    }

    SiteConfiguration siteCfg = EasyMock.createMockBuilder(SiteConfiguration.class).addMockedMethod("getHadoopConfiguration")
        .withConstructor(AccumuloConfiguration.class).withArgs(DefaultConfiguration.getInstance()).createMock();

    siteCfg.set(Property.INSTANCE_SECRET, "ignored");

    // site-cfg.jceks={'ignored.property'=>'ignored', 'instance.secret'=>'mysecret', 'general.rpc.timeout'=>'timeout'}
    URL keystore = SiteConfigurationTest.class.getResource("/site-cfg.jceks");
    Assert.assertNotNull(keystore);
    String keystorePath = new File(keystore.getFile()).getAbsolutePath();

    Configuration hadoopConf = new Configuration();
    hadoopConf.set(CredentialProviderFactoryShim.CREDENTIAL_PROVIDER_PATH, "jceks://file" + keystorePath);

    EasyMock.expect(siteCfg.getHadoopConfiguration()).andReturn(hadoopConf).once();

    EasyMock.replay(siteCfg);

    Map<String,String> props = new HashMap<String,String>();
    siteCfg.getProperties(props, new AllFilter());

    Assert.assertEquals("mysecret", props.get(Property.INSTANCE_SECRET.getKey()));
    Assert.assertEquals(null, props.get("ignored.property"));
    Assert.assertEquals(Property.GENERAL_RPC_TIMEOUT.getDefaultValue(), props.get(Property.GENERAL_RPC_TIMEOUT.getKey()));
  }

}
