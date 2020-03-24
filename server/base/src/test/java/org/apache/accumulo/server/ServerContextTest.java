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
package org.apache.accumulo.server;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.security.PrivilegedExceptionAction;
import java.util.Properties;

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
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

public class ServerContextTest {

  private UserGroupInformation testUser;
  private String username;

  @Before
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
      SiteConfiguration siteConfig = EasyMock.createMock(SiteConfiguration.class);

      EasyMock.expect(siteConfig.getBoolean(Property.INSTANCE_RPC_SASL_ENABLED)).andReturn(true);

      // Deal with SystemToken being private
      PasswordToken pw = new PasswordToken("fake");
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      pw.write(new DataOutputStream(baos));
      SystemToken token = new SystemToken();
      token.readFields(new DataInputStream(new ByteArrayInputStream(baos.toByteArray())));

      ServerConfigurationFactory factory = EasyMock.createMock(ServerConfigurationFactory.class);
      EasyMock.expect(factory.getSystemConfiguration()).andReturn(conf).anyTimes();
      EasyMock.expect(factory.getSiteConfiguration()).andReturn(siteConfig).anyTimes();

      ServerContext context = EasyMock.createMockBuilder(ServerContext.class)
          .addMockedMethod("enforceKerberosLogin").addMockedMethod("getConfiguration")
          .addMockedMethod("getServerConfFactory").addMockedMethod("getCredentials").createMock();
      context.enforceKerberosLogin();
      EasyMock.expectLastCall().anyTimes();
      EasyMock.expect(context.getConfiguration()).andReturn(conf).anyTimes();
      EasyMock.expect(context.getServerConfFactory()).andReturn(factory).anyTimes();
      EasyMock.expect(context.getCredentials())
          .andReturn(new Credentials("accumulo/hostname@FAKE.COM", token)).once();

      // Just make the SiteConfiguration delegate to our ClientConfiguration (by way of the
      // AccumuloConfiguration)
      // Presently, we only need get(Property) and iterator().
      EasyMock.expect(siteConfig.get(EasyMock.anyObject(Property.class))).andAnswer(() -> {
        Object[] args = EasyMock.getCurrentArguments();
        return conf.get((Property) args[0]);
      }).anyTimes();

      EasyMock.expect(siteConfig.iterator()).andAnswer(conf::iterator).anyTimes();

      EasyMock.replay(factory, context, siteConfig);

      assertEquals(ThriftServerType.SASL, context.getThriftServerType());
      SaslServerConnectionParams saslParams = context.getSaslParams();
      assertEquals(new SaslServerConnectionParams(conf, token), saslParams);
      assertEquals(username, saslParams.getPrincipal());

      EasyMock.verify(factory, context, siteConfig);

      return null;
    });
  }

}
