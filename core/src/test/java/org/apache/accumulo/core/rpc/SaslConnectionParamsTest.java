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
package org.apache.accumulo.core.rpc;

import static org.apache.accumulo.core.clientImpl.AuthenticationTokenIdentifier.createTAuthIdentifier;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.security.PrivilegedExceptionAction;
import java.util.Map;
import java.util.Properties;

import javax.security.sasl.Sasl;

import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.clientImpl.AuthenticationTokenIdentifier;
import org.apache.accumulo.core.clientImpl.DelegationTokenImpl;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.rpc.SaslConnectionParams.QualityOfProtection;
import org.apache.accumulo.core.rpc.SaslConnectionParams.SaslMechanism;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.UserGroupInformation;
import org.easymock.EasyMock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SaslConnectionParamsTest {

  private UserGroupInformation testUser;
  private String username;
  private static final String primary = "accumulo";

  @BeforeEach
  public void setup() {
    System.setProperty("java.security.krb5.realm", "accumulo");
    System.setProperty("java.security.krb5.kdc", "fake");
    Configuration conf = new Configuration(false);
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    UserGroupInformation.setConfiguration(conf);
    testUser = UserGroupInformation.createUserForTesting("test_user", new String[0]);
    username = testUser.getUserName();
  }

  private static SaslConnectionParams createSaslParams(AuthenticationToken token) {
    Properties props = new Properties();
    props.setProperty(ClientProperty.SASL_KERBEROS_SERVER_PRIMARY.getKey(), primary);
    props.setProperty(ClientProperty.SASL_ENABLED.getKey(), "true");
    return new SaslConnectionParams(props, token);
  }

  @Test
  public void testDefaultParamsAsClient() throws Exception {
    final KerberosToken token = EasyMock.createMock(KerberosToken.class);
    testUser.doAs((PrivilegedExceptionAction<Void>) () -> {
      final SaslConnectionParams saslParams = createSaslParams(token);
      assertEquals(primary, saslParams.getKerberosServerPrimary());

      final QualityOfProtection defaultQop =
          QualityOfProtection.get(Property.RPC_SASL_QOP.getDefaultValue());
      assertEquals(defaultQop, saslParams.getQualityOfProtection());

      Map<String,String> properties = saslParams.getSaslProperties();
      assertEquals(1, properties.size());
      assertEquals(defaultQop.getQuality(), properties.get(Sasl.QOP));
      assertEquals(username, saslParams.getPrincipal());
      return null;
    });
  }

  @Test
  public void testDefaultParams() throws Exception {
    final KerberosToken token = EasyMock.createMock(KerberosToken.class);
    testUser.doAs((PrivilegedExceptionAction<Void>) () -> {
      final SaslConnectionParams saslParams = createSaslParams(token);
      assertEquals(primary, saslParams.getKerberosServerPrimary());

      final QualityOfProtection defaultQop =
          QualityOfProtection.get(Property.RPC_SASL_QOP.getDefaultValue());
      assertEquals(defaultQop, saslParams.getQualityOfProtection());

      Map<String,String> properties = saslParams.getSaslProperties();
      assertEquals(1, properties.size());
      assertEquals(defaultQop.getQuality(), properties.get(Sasl.QOP));
      assertEquals(username, saslParams.getPrincipal());
      return null;
    });
  }

  @Test
  public void testDelegationTokenImpl() throws Exception {
    final DelegationTokenImpl token =
        new DelegationTokenImpl(new byte[0], new AuthenticationTokenIdentifier(
            createTAuthIdentifier("user", 1, 10L, 20L, "instanceid")));
    testUser.doAs((PrivilegedExceptionAction<Void>) () -> {
      final SaslConnectionParams saslParams = createSaslParams(token);
      assertEquals(primary, saslParams.getKerberosServerPrimary());

      final QualityOfProtection defaultQop =
          QualityOfProtection.get(Property.RPC_SASL_QOP.getDefaultValue());
      assertEquals(defaultQop, saslParams.getQualityOfProtection());

      assertEquals(SaslMechanism.DIGEST_MD5, saslParams.getMechanism());
      assertNotNull(saslParams.getCallbackHandler());
      assertEquals(SaslClientDigestCallbackHandler.class,
          saslParams.getCallbackHandler().getClass());

      Map<String,String> properties = saslParams.getSaslProperties();
      assertEquals(1, properties.size());
      assertEquals(defaultQop.getQuality(), properties.get(Sasl.QOP));
      assertEquals(username, saslParams.getPrincipal());
      return null;
    });
  }

  @Test
  public void testEquality() throws Exception {
    final KerberosToken token = EasyMock.createMock(KerberosToken.class);
    SaslConnectionParams params1 = testUser
        .doAs((PrivilegedExceptionAction<SaslConnectionParams>) () -> createSaslParams(token));

    SaslConnectionParams params2 = testUser
        .doAs((PrivilegedExceptionAction<SaslConnectionParams>) () -> createSaslParams(token));

    assertEquals(params1, params2);
    assertEquals(params1.hashCode(), params2.hashCode());

    final DelegationTokenImpl delToken1 =
        new DelegationTokenImpl(new byte[0], new AuthenticationTokenIdentifier(
            createTAuthIdentifier("user", 1, 10L, 20L, "instanceid")));
    SaslConnectionParams params3 = testUser
        .doAs((PrivilegedExceptionAction<SaslConnectionParams>) () -> createSaslParams(delToken1));

    assertNotEquals(params1, params3);
    assertNotEquals(params1.hashCode(), params3.hashCode());
    assertNotEquals(params2, params3);
    assertNotEquals(params2.hashCode(), params3.hashCode());

    final DelegationTokenImpl delToken2 =
        new DelegationTokenImpl(new byte[0], new AuthenticationTokenIdentifier(
            createTAuthIdentifier("user", 1, 10L, 20L, "instanceid")));
    SaslConnectionParams params4 = testUser
        .doAs((PrivilegedExceptionAction<SaslConnectionParams>) () -> createSaslParams(delToken2));

    assertNotEquals(params1, params4);
    assertNotEquals(params1.hashCode(), params4.hashCode());
    assertNotEquals(params2, params4);
    assertNotEquals(params2.hashCode(), params4.hashCode());

    assertEquals(params3, params4);
    assertEquals(params3.hashCode(), params4.hashCode());
  }
}
