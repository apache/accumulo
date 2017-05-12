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
package org.apache.accumulo.server;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.security.PrivilegedExceptionAction;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.ClientConfiguration.ClientProperty;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.client.impl.Credentials;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
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
import org.easymock.IAnswer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AccumuloServerContextTest {

  private UserGroupInformation testUser;
  private String username;

  @Before
  public void setup() throws Exception {
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

    testUser.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        Instance instance = EasyMock.createMock(Instance.class);

        ClientConfiguration clientConf = ClientConfiguration.loadDefault();
        clientConf.setProperty(ClientProperty.INSTANCE_RPC_SASL_ENABLED, "true");
        clientConf.setProperty(ClientProperty.KERBEROS_SERVER_PRIMARY, "accumulo");
        final AccumuloConfiguration conf = ClientContext.convertClientConfig(clientConf);
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
        EasyMock.expect(factory.getInstance()).andReturn(instance).anyTimes();

        AccumuloServerContext context = EasyMock.createMockBuilder(AccumuloServerContext.class).addMockedMethod("enforceKerberosLogin")
            .addMockedMethod("getConfiguration").addMockedMethod("getServerConfigurationFactory").addMockedMethod("getCredentials").createMock();
        context.enforceKerberosLogin();
        EasyMock.expectLastCall().anyTimes();
        EasyMock.expect(context.getConfiguration()).andReturn(conf).anyTimes();
        EasyMock.expect(context.getServerConfigurationFactory()).andReturn(factory).anyTimes();
        EasyMock.expect(context.getCredentials()).andReturn(new Credentials("accumulo/hostname@FAKE.COM", token)).once();

        // Just make the SiteConfiguration delegate to our ClientConfiguration (by way of the AccumuloConfiguration)
        // Presently, we only need get(Property) and iterator().
        EasyMock.expect(siteConfig.get(EasyMock.anyObject(Property.class))).andAnswer(new IAnswer<String>() {
          @Override
          public String answer() {
            Object[] args = EasyMock.getCurrentArguments();
            return conf.get((Property) args[0]);
          }
        }).anyTimes();

        EasyMock.expect(siteConfig.iterator()).andAnswer(new IAnswer<Iterator<Entry<String,String>>>() {
          @Override
          public Iterator<Entry<String,String>> answer() {
            return conf.iterator();
          }
        }).anyTimes();

        EasyMock.replay(factory, context, siteConfig);

        Assert.assertEquals(ThriftServerType.SASL, context.getThriftServerType());
        SaslServerConnectionParams saslParams = context.getSaslParams();
        Assert.assertEquals(new SaslServerConnectionParams(conf, token), saslParams);
        Assert.assertEquals(username, saslParams.getPrincipal());

        EasyMock.verify(factory, context, siteConfig);

        return null;
      }
    });
  }

}
