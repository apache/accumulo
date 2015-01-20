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
package org.apache.accumulo.core.rpc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Map;

import javax.security.sasl.Sasl;

import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.ClientConfiguration.ClientProperty;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.rpc.SaslConnectionParams.QualityOfProtection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Before;
import org.junit.Test;

public class SaslConnectionParamsTest {

  private String user;

  @Before
  public void setup() throws Exception {
    System.setProperty("java.security.krb5.realm", "accumulo");
    System.setProperty("java.security.krb5.kdc", "fake");
    Configuration conf = new Configuration(false);
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    UserGroupInformation.setConfiguration(conf);
    user = UserGroupInformation.getCurrentUser().getUserName();
  }

  @Test
  public void testNullParams() {
    ClientConfiguration clientConf = new ClientConfiguration();
    AccumuloConfiguration rpcConf = ClientContext.convertClientConfig(clientConf);
    assertEquals("false", clientConf.get(ClientProperty.INSTANCE_RPC_SASL_ENABLED));
    assertNull(SaslConnectionParams.forConfig(rpcConf));
  }

  @Test
  public void testDefaultParamsAsClient() throws Exception {
    final ClientConfiguration clientConf = ClientConfiguration.loadDefault();

    // The primary is the first component of the principal
    final String primary = "accumulo";
    clientConf.withSasl(true, primary);

    assertEquals("true", clientConf.get(ClientProperty.INSTANCE_RPC_SASL_ENABLED));

    final SaslConnectionParams saslParams = SaslConnectionParams.forConfig(clientConf);
    assertEquals(primary, saslParams.getKerberosServerPrimary());

    final QualityOfProtection defaultQop = QualityOfProtection.get(Property.RPC_SASL_QOP.getDefaultValue());
    assertEquals(defaultQop, saslParams.getQualityOfProtection());

    Map<String,String> properties = saslParams.getSaslProperties();
    assertEquals(1, properties.size());
    assertEquals(defaultQop.getQuality(), properties.get(Sasl.QOP));
    assertEquals(user, saslParams.getPrincipal());
  }

  @Test
  public void testDefaultParamsAsServer() throws Exception {
    final ClientConfiguration clientConf = ClientConfiguration.loadDefault();

    // The primary is the first component of the principal
    final String primary = "accumulo";
    clientConf.withSasl(true, primary);

    final AccumuloConfiguration rpcConf = ClientContext.convertClientConfig(clientConf);
    assertEquals("true", clientConf.get(ClientProperty.INSTANCE_RPC_SASL_ENABLED));

    final SaslConnectionParams saslParams = SaslConnectionParams.forConfig(rpcConf);
    assertEquals(primary, saslParams.getKerberosServerPrimary());

    final QualityOfProtection defaultQop = QualityOfProtection.get(Property.RPC_SASL_QOP.getDefaultValue());
    assertEquals(defaultQop, saslParams.getQualityOfProtection());

    Map<String,String> properties = saslParams.getSaslProperties();
    assertEquals(1, properties.size());
    assertEquals(defaultQop.getQuality(), properties.get(Sasl.QOP));
    assertEquals(user, saslParams.getPrincipal());
  }

}
