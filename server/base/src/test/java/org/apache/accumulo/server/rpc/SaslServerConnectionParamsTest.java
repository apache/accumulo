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
package org.apache.accumulo.server.rpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import java.util.Properties;

import javax.security.sasl.Sasl;

import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.clientImpl.ClientConfConverter;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.rpc.SaslConnectionParams;
import org.apache.accumulo.core.rpc.SaslConnectionParams.QualityOfProtection;
import org.apache.accumulo.core.rpc.SaslConnectionParams.SaslMechanism;
import org.apache.accumulo.server.security.SystemCredentials.SystemToken;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SaslServerConnectionParamsTest {

  private UserGroupInformation testUser;
  private String username;

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

  @Test
  public void testDefaultParamsAsServer() throws Exception {
    testUser.doAs((PrivilegedExceptionAction<Void>) () -> {
      Properties clientProps = new Properties();
      clientProps.setProperty(ClientProperty.SASL_ENABLED.getKey(), "true");
      final String primary = "accumulo";
      clientProps.setProperty(ClientProperty.SASL_KERBEROS_SERVER_PRIMARY.getKey(), primary);

      final AccumuloConfiguration rpcConf = ClientConfConverter.toAccumuloConf(clientProps);
      assertEquals("true", rpcConf.get(Property.INSTANCE_RPC_SASL_ENABLED));

      // Deal with SystemToken being private
      PasswordToken pw = new PasswordToken("fake");
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      pw.write(new DataOutputStream(baos));
      SystemToken token = new SystemToken();
      token.readFields(new DataInputStream(new ByteArrayInputStream(baos.toByteArray())));

      final SaslConnectionParams saslParams = new SaslServerConnectionParams(rpcConf, token, null);
      assertEquals(primary, saslParams.getKerberosServerPrimary());
      assertEquals(SaslMechanism.GSSAPI, saslParams.getMechanism());
      assertNull(saslParams.getCallbackHandler());

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

}
