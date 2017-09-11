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

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.ClientConfiguration.ClientProperty;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.rpc.SaslConnectionParams;
import org.apache.accumulo.core.rpc.SslConnectionParams;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.UserGroupInformation;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

public class ThriftTransportKeyTest {

  @Before
  public void setup() throws Exception {
    System.setProperty("java.security.krb5.realm", "accumulo");
    System.setProperty("java.security.krb5.kdc", "fake");
    Configuration conf = new Configuration(false);
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    UserGroupInformation.setConfiguration(conf);
  }

  @Test(expected = RuntimeException.class)
  public void testSslAndSaslErrors() {
    ClientContext clientCtx = createMock(ClientContext.class);
    SslConnectionParams sslParams = createMock(SslConnectionParams.class);
    SaslConnectionParams saslParams = createMock(SaslConnectionParams.class);

    expect(clientCtx.getClientSslParams()).andReturn(sslParams).anyTimes();
    expect(clientCtx.getSaslParams()).andReturn(saslParams).anyTimes();

    // We don't care to verify the sslparam or saslparam mocks
    replay(clientCtx);

    try {
      new ThriftTransportKey(HostAndPort.fromParts("localhost", 9999), 120 * 1000, clientCtx);
    } finally {
      verify(clientCtx);
    }
  }

  @Test
  public void testConnectionCaching() throws IOException, InterruptedException {
    UserGroupInformation user1 = UserGroupInformation.createUserForTesting("user1", new String[0]);
    final KerberosToken token = EasyMock.createMock(KerberosToken.class);
    final ClientConfiguration clientConf = ClientConfiguration.loadDefault();
    // The primary is the first component of the principal
    final String primary = "accumulo";
    clientConf.withSasl(true, primary);

    // A first instance of the SASL cnxn params
    SaslConnectionParams saslParams1 = user1.doAs(new PrivilegedExceptionAction<SaslConnectionParams>() {
      @Override
      public SaslConnectionParams run() throws Exception {
        return new SaslConnectionParams(clientConf, token);
      }
    });

    // A second instance of what should be the same SaslConnectionParams
    SaslConnectionParams saslParams2 = user1.doAs(new PrivilegedExceptionAction<SaslConnectionParams>() {
      @Override
      public SaslConnectionParams run() throws Exception {
        return new SaslConnectionParams(clientConf, token);
      }
    });

    ThriftTransportKey ttk1 = new ThriftTransportKey(HostAndPort.fromParts("localhost", 9997), 1l, null, saslParams1), ttk2 = new ThriftTransportKey(
        HostAndPort.fromParts("localhost", 9997), 1l, null, saslParams2);

    // Should equals() and hashCode() to make sure we don't throw away thrift cnxns
    assertEquals(ttk1, ttk2);
    assertEquals(ttk1.hashCode(), ttk2.hashCode());
  }

  @Test
  public void testSaslPrincipalIsSignificant() throws IOException, InterruptedException {
    UserGroupInformation user1 = UserGroupInformation.createUserForTesting("user1", new String[0]);
    final KerberosToken token = EasyMock.createMock(KerberosToken.class);
    SaslConnectionParams saslParams1 = user1.doAs(new PrivilegedExceptionAction<SaslConnectionParams>() {
      @Override
      public SaslConnectionParams run() throws Exception {
        final ClientConfiguration clientConf = ClientConfiguration.loadDefault();

        // The primary is the first component of the principal
        final String primary = "accumulo";
        clientConf.withSasl(true, primary);

        assertEquals("true", clientConf.get(ClientProperty.INSTANCE_RPC_SASL_ENABLED));

        return new SaslConnectionParams(clientConf, token);
      }
    });

    UserGroupInformation user2 = UserGroupInformation.createUserForTesting("user2", new String[0]);
    SaslConnectionParams saslParams2 = user2.doAs(new PrivilegedExceptionAction<SaslConnectionParams>() {
      @Override
      public SaslConnectionParams run() throws Exception {
        final ClientConfiguration clientConf = ClientConfiguration.loadDefault();

        // The primary is the first component of the principal
        final String primary = "accumulo";
        clientConf.withSasl(true, primary);

        assertEquals("true", clientConf.get(ClientProperty.INSTANCE_RPC_SASL_ENABLED));

        return new SaslConnectionParams(clientConf, token);
      }
    });

    ThriftTransportKey ttk1 = new ThriftTransportKey(HostAndPort.fromParts("localhost", 9997), 1l, null, saslParams1), ttk2 = new ThriftTransportKey(
        HostAndPort.fromParts("localhost", 9997), 1l, null, saslParams2);

    assertNotEquals(ttk1, ttk2);
    assertNotEquals(ttk1.hashCode(), ttk2.hashCode());
  }

  @Test
  public void testSimpleEquivalence() {
    ClientContext clientCtx = createMock(ClientContext.class);

    expect(clientCtx.getClientSslParams()).andReturn(null).anyTimes();
    expect(clientCtx.getSaslParams()).andReturn(null).anyTimes();

    replay(clientCtx);

    ThriftTransportKey ttk = new ThriftTransportKey(HostAndPort.fromParts("localhost", 9999), 120 * 1000, clientCtx);

    assertTrue("Normal ThriftTransportKey doesn't equal itself", ttk.equals(ttk));
  }

}
