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
package org.apache.accumulo.server;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.createMockBuilder;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.getCurrentArguments;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.security.PrivilegedExceptionAction;
import java.util.Properties;
import java.util.function.IntConsumer;
import java.util.stream.IntStream;

import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.clientImpl.ClientConfConverter;
import org.apache.accumulo.core.clientImpl.Credentials;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.accumulo.server.rpc.SaslServerConnectionParams;
import org.apache.accumulo.server.rpc.ThriftServerType;
import org.apache.accumulo.server.security.SystemCredentials.SystemToken;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ServerContextTest {

  private UserGroupInformation testUser;
  private String username;

  @BeforeEach
  public void setup() {
    System.setProperty("java.security.krb5.realm", "accumulo");
    System.setProperty("java.security.krb5.kdc", "fake");
    Configuration conf = new Configuration(false);
    conf.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    UserGroupInformation.setConfiguration(conf);
    testUser = UserGroupInformation.createUserForTesting("test_user", new String[0]);
    username = testUser.getUserName();
  }

  @Test
  public void testSasl() throws Exception {

    testUser.doAs((PrivilegedExceptionAction<Void>) () -> {

      Properties clientProps = new Properties();
      clientProps.setProperty(ClientProperty.SASL_ENABLED.getKey(), "true");
      clientProps.setProperty(ClientProperty.SASL_KERBEROS_SERVER_PRIMARY.getKey(), "accumulo");
      final AccumuloConfiguration conf = ClientConfConverter.toAccumuloConf(clientProps);
      SiteConfiguration siteConfig = createMock(SiteConfiguration.class);

      expect(siteConfig.getBoolean(Property.INSTANCE_RPC_SASL_ENABLED)).andReturn(true);

      // Deal with SystemToken being private
      PasswordToken pw = new PasswordToken("fake");
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      pw.write(new DataOutputStream(baos));
      SystemToken token = new SystemToken();
      token.readFields(new DataInputStream(new ByteArrayInputStream(baos.toByteArray())));

      ServerConfigurationFactory factory = createMock(ServerConfigurationFactory.class);
      expect(factory.getSystemConfiguration()).andReturn(conf).anyTimes();

      ServerContext context =
          createMockBuilder(ServerContext.class).addMockedMethod("enforceKerberosLogin")
              .addMockedMethod("getConfiguration").addMockedMethod("getSiteConfiguration")
              .addMockedMethod("getSecretManager").addMockedMethod("getCredentials").createMock();
      context.enforceKerberosLogin();
      expectLastCall().anyTimes();
      expect(context.getSiteConfiguration()).andReturn(siteConfig).anyTimes();
      expect(context.getSecretManager()).andReturn(null).anyTimes();
      expect(context.getConfiguration()).andReturn(conf).anyTimes();
      expect(context.getCredentials())
          .andReturn(new Credentials("accumulo/hostname@FAKE.COM", token)).once();

      // Just make the SiteConfiguration delegate to our ClientConfiguration (by way of the
      // AccumuloConfiguration)
      // Presently, we only need get(Property) and iterator().
      expect(siteConfig.get(anyObject(Property.class))).andAnswer(() -> {
        Object[] args = getCurrentArguments();
        return conf.get((Property) args[0]);
      }).anyTimes();

      expect(siteConfig.iterator()).andAnswer(conf::iterator).anyTimes();
      expect(siteConfig.stream()).andAnswer(conf::stream).anyTimes();

      replay(factory, context, siteConfig);

      assertEquals(ThriftServerType.SASL, context.getThriftServerType());
      SaslServerConnectionParams saslParams = context.getSaslParams();
      assertEquals(new SaslServerConnectionParams(conf, token, null), saslParams);
      assertEquals(username, saslParams.getPrincipal());

      verify(factory, context, siteConfig);

      return null;
    });
  }

  @Test
  public void testCanRun() {
    // ensure this fails with older versions; the oldest supported version is hard-coded here
    // to ensure we don't unintentionally break upgrade support; changing this should be a conscious
    // decision and this check will ensure we don't overlook it
    final int oldestSupported = 8;
    final int currentVersion = AccumuloDataVersion.get();
    IntConsumer shouldPass = ServerContext::ensureDataVersionCompatible;
    IntConsumer shouldFail = v -> assertThrows(IllegalStateException.class,
        () -> ServerContext.ensureDataVersionCompatible(v));
    IntStream.rangeClosed(oldestSupported, currentVersion).forEach(shouldPass);
    IntStream.of(oldestSupported - 1, currentVersion + 1).forEach(shouldFail);
  }

}
