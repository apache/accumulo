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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

import java.security.PrivilegedExceptionAction;
import java.util.Map;

import javax.security.sasl.Sasl;

import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.ClientConfiguration.ClientProperty;
import org.apache.accumulo.core.client.impl.AuthenticationTokenIdentifier;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.client.impl.DelegationTokenImpl;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.rpc.SaslConnectionParams.QualityOfProtection;
import org.apache.accumulo.core.rpc.SaslConnectionParams.SaslMechanism;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.UserGroupInformation;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

public class SaslConnectionParamsTest {

  private UserGroupInformation testUser;
  private String username;

  @Before
  public void setup() throws Exception {
    System.setProperty("java.security.krb5.realm", "accumulo");
    System.setProperty("java.security.krb5.kdc", "fake");
    Configuration conf = new Configuration(false);
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    UserGroupInformation.setConfiguration(conf);
    testUser = UserGroupInformation.createUserForTesting("test_user", new String[0]);
    username = testUser.getUserName();
  }

  @Test
  public void testDefaultParamsAsClient() throws Exception {
    final KerberosToken token = EasyMock.createMock(KerberosToken.class);
    testUser.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        final ClientConfiguration clientConf = ClientConfiguration.loadDefault();

        // The primary is the first component of the principal
        final String primary = "accumulo";
        clientConf.withSasl(true, primary);

        assertEquals("true", clientConf.get(ClientProperty.INSTANCE_RPC_SASL_ENABLED));

        final SaslConnectionParams saslParams = new SaslConnectionParams(clientConf, token);
        assertEquals(primary, saslParams.getKerberosServerPrimary());

        final QualityOfProtection defaultQop = QualityOfProtection.get(Property.RPC_SASL_QOP.getDefaultValue());
        assertEquals(defaultQop, saslParams.getQualityOfProtection());

        Map<String,String> properties = saslParams.getSaslProperties();
        assertEquals(1, properties.size());
        assertEquals(defaultQop.getQuality(), properties.get(Sasl.QOP));
        assertEquals(username, saslParams.getPrincipal());
        return null;
      }
    });
  }

  @Test
  public void testDefaultParams() throws Exception {
    final KerberosToken token = EasyMock.createMock(KerberosToken.class);
    testUser.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        final ClientConfiguration clientConf = ClientConfiguration.loadDefault();

        // The primary is the first component of the principal
        final String primary = "accumulo";
        clientConf.withSasl(true, primary);

        final AccumuloConfiguration rpcConf = ClientContext.convertClientConfig(clientConf);
        assertEquals("true", clientConf.get(ClientProperty.INSTANCE_RPC_SASL_ENABLED));

        final SaslConnectionParams saslParams = new SaslConnectionParams(rpcConf, token);
        assertEquals(primary, saslParams.getKerberosServerPrimary());

        final QualityOfProtection defaultQop = QualityOfProtection.get(Property.RPC_SASL_QOP.getDefaultValue());
        assertEquals(defaultQop, saslParams.getQualityOfProtection());

        Map<String,String> properties = saslParams.getSaslProperties();
        assertEquals(1, properties.size());
        assertEquals(defaultQop.getQuality(), properties.get(Sasl.QOP));
        assertEquals(username, saslParams.getPrincipal());
        return null;
      }
    });
  }

  @Test
  public void testDelegationTokenImpl() throws Exception {
    final DelegationTokenImpl token = new DelegationTokenImpl(new byte[0], new AuthenticationTokenIdentifier("user", 1, 10l, 20l, "instanceid"));
    testUser.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        final ClientConfiguration clientConf = ClientConfiguration.loadDefault();

        // The primary is the first component of the principal
        final String primary = "accumulo";
        clientConf.withSasl(true, primary);

        final AccumuloConfiguration rpcConf = ClientContext.convertClientConfig(clientConf);
        assertEquals("true", clientConf.get(ClientProperty.INSTANCE_RPC_SASL_ENABLED));

        final SaslConnectionParams saslParams = new SaslConnectionParams(rpcConf, token);
        assertEquals(primary, saslParams.getKerberosServerPrimary());

        final QualityOfProtection defaultQop = QualityOfProtection.get(Property.RPC_SASL_QOP.getDefaultValue());
        assertEquals(defaultQop, saslParams.getQualityOfProtection());

        assertEquals(SaslMechanism.DIGEST_MD5, saslParams.getMechanism());
        assertNotNull(saslParams.getCallbackHandler());
        assertEquals(SaslClientDigestCallbackHandler.class, saslParams.getCallbackHandler().getClass());

        Map<String,String> properties = saslParams.getSaslProperties();
        assertEquals(1, properties.size());
        assertEquals(defaultQop.getQuality(), properties.get(Sasl.QOP));
        assertEquals(username, saslParams.getPrincipal());
        return null;
      }
    });
  }

  @Test
  public void testEquality() throws Exception {
    final KerberosToken token = EasyMock.createMock(KerberosToken.class);
    SaslConnectionParams params1 = testUser.doAs(new PrivilegedExceptionAction<SaslConnectionParams>() {
      @Override
      public SaslConnectionParams run() throws Exception {
        final ClientConfiguration clientConf = ClientConfiguration.loadDefault();

        // The primary is the first component of the principal
        final String primary = "accumulo";
        clientConf.withSasl(true, primary);

        final AccumuloConfiguration rpcConf = ClientContext.convertClientConfig(clientConf);
        assertEquals("true", clientConf.get(ClientProperty.INSTANCE_RPC_SASL_ENABLED));

        return new SaslConnectionParams(rpcConf, token);
      }
    });

    SaslConnectionParams params2 = testUser.doAs(new PrivilegedExceptionAction<SaslConnectionParams>() {
      @Override
      public SaslConnectionParams run() throws Exception {
        final ClientConfiguration clientConf = ClientConfiguration.loadDefault();

        // The primary is the first component of the principal
        final String primary = "accumulo";
        clientConf.withSasl(true, primary);

        final AccumuloConfiguration rpcConf = ClientContext.convertClientConfig(clientConf);
        assertEquals("true", clientConf.get(ClientProperty.INSTANCE_RPC_SASL_ENABLED));

        return new SaslConnectionParams(rpcConf, token);
      }
    });

    assertEquals(params1, params2);
    assertEquals(params1.hashCode(), params2.hashCode());

    final DelegationTokenImpl delToken1 = new DelegationTokenImpl(new byte[0], new AuthenticationTokenIdentifier("user", 1, 10l, 20l, "instanceid"));
    SaslConnectionParams params3 = testUser.doAs(new PrivilegedExceptionAction<SaslConnectionParams>() {
      @Override
      public SaslConnectionParams run() throws Exception {
        final ClientConfiguration clientConf = ClientConfiguration.loadDefault();

        // The primary is the first component of the principal
        final String primary = "accumulo";
        clientConf.withSasl(true, primary);

        final AccumuloConfiguration rpcConf = ClientContext.convertClientConfig(clientConf);
        assertEquals("true", clientConf.get(ClientProperty.INSTANCE_RPC_SASL_ENABLED));

        return new SaslConnectionParams(rpcConf, delToken1);
      }
    });

    assertNotEquals(params1, params3);
    assertNotEquals(params1.hashCode(), params3.hashCode());
    assertNotEquals(params2, params3);
    assertNotEquals(params2.hashCode(), params3.hashCode());

    final DelegationTokenImpl delToken2 = new DelegationTokenImpl(new byte[0], new AuthenticationTokenIdentifier("user", 1, 10l, 20l, "instanceid"));
    SaslConnectionParams params4 = testUser.doAs(new PrivilegedExceptionAction<SaslConnectionParams>() {
      @Override
      public SaslConnectionParams run() throws Exception {
        final ClientConfiguration clientConf = ClientConfiguration.loadDefault();

        // The primary is the first component of the principal
        final String primary = "accumulo";
        clientConf.withSasl(true, primary);

        final AccumuloConfiguration rpcConf = ClientContext.convertClientConfig(clientConf);
        assertEquals("true", clientConf.get(ClientProperty.INSTANCE_RPC_SASL_ENABLED));

        return new SaslConnectionParams(rpcConf, delToken2);
      }
    });

    assertNotEquals(params1, params4);
    assertNotEquals(params1.hashCode(), params4.hashCode());
    assertNotEquals(params2, params4);
    assertNotEquals(params2.hashCode(), params4.hashCode());

    assertEquals(params3, params4);
    assertEquals(params3.hashCode(), params4.hashCode());
  }
}
