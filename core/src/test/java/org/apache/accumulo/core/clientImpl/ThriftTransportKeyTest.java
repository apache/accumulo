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
package org.apache.accumulo.core.clientImpl;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Properties;

import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.rpc.SaslConnectionParams;
import org.apache.accumulo.core.rpc.SslConnectionParams;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.UserGroupInformation;
import org.easymock.EasyMock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ThriftTransportKeyTest {

  private static final String primary = "accumulo";

  @BeforeEach
  public void setup() {
    System.setProperty("java.security.krb5.realm", "accumulo");
    System.setProperty("java.security.krb5.kdc", "fake");
    Configuration conf = new Configuration(false);
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    UserGroupInformation.setConfiguration(conf);
  }

  private static SaslConnectionParams createSaslParams(AuthenticationToken token) {
    Properties props = new Properties();
    props.setProperty(ClientProperty.SASL_KERBEROS_SERVER_PRIMARY.getKey(), primary);
    props.setProperty(ClientProperty.SASL_ENABLED.getKey(), "true");
    return new SaslConnectionParams(props, token);
  }

  @Test
  public void testSslAndSaslErrors() {
    ClientContext clientCtx = createMock(ClientContext.class);
    SslConnectionParams sslParams = createMock(SslConnectionParams.class);
    SaslConnectionParams saslParams = createMock(SaslConnectionParams.class);

    expect(clientCtx.getClientSslParams()).andReturn(sslParams).anyTimes();
    expect(clientCtx.getSaslParams()).andReturn(saslParams).anyTimes();

    // We don't care to verify the sslparam or saslparam mocks
    replay(clientCtx);

    try {
      assertThrows(RuntimeException.class, () -> new ThriftTransportKey(ThriftClientTypes.CLIENT,
          HostAndPort.fromParts("localhost", 9999), 120_000, clientCtx));
    } finally {
      verify(clientCtx);
    }
  }

  @Test
  public void testConnectionCaching() throws IOException, InterruptedException {
    UserGroupInformation user1 = UserGroupInformation.createUserForTesting("user1", new String[0]);
    final KerberosToken token = EasyMock.createMock(KerberosToken.class);

    // A first instance of the SASL cnxn params
    SaslConnectionParams saslParams1 =
        user1.doAs((PrivilegedExceptionAction<SaslConnectionParams>) () -> createSaslParams(token));

    // A second instance of what should be the same SaslConnectionParams
    SaslConnectionParams saslParams2 =
        user1.doAs((PrivilegedExceptionAction<SaslConnectionParams>) () -> createSaslParams(token));

    ThriftTransportKey ttk1 =
        new ThriftTransportKey(ThriftClientTypes.CLIENT, HostAndPort.fromParts("localhost", 9997),
            1L, null, saslParams1),
        ttk2 = new ThriftTransportKey(ThriftClientTypes.CLIENT,
            HostAndPort.fromParts("localhost", 9997), 1L, null, saslParams2);

    // Should equals() and hashCode() to make sure we don't throw away thrift cnxns
    assertEquals(ttk1, ttk2);
    assertEquals(ttk1.hashCode(), ttk2.hashCode());
  }

  @Test
  public void testSaslPrincipalIsSignificant() throws IOException, InterruptedException {
    UserGroupInformation user1 = UserGroupInformation.createUserForTesting("user1", new String[0]);
    final KerberosToken token = EasyMock.createMock(KerberosToken.class);
    SaslConnectionParams saslParams1 =
        user1.doAs((PrivilegedExceptionAction<SaslConnectionParams>) () -> createSaslParams(token));

    UserGroupInformation user2 = UserGroupInformation.createUserForTesting("user2", new String[0]);
    SaslConnectionParams saslParams2 =
        user2.doAs((PrivilegedExceptionAction<SaslConnectionParams>) () -> createSaslParams(token));

    ThriftTransportKey ttk1 =
        new ThriftTransportKey(ThriftClientTypes.CLIENT, HostAndPort.fromParts("localhost", 9997),
            1L, null, saslParams1),
        ttk2 = new ThriftTransportKey(ThriftClientTypes.CLIENT,
            HostAndPort.fromParts("localhost", 9997), 1L, null, saslParams2);

    assertNotEquals(ttk1, ttk2);
    assertNotEquals(ttk1.hashCode(), ttk2.hashCode());
  }

  @Test
  public void testSimpleEquivalence() {
    ClientContext clientCtx = createMock(ClientContext.class);

    expect(clientCtx.getClientSslParams()).andReturn(null).anyTimes();
    expect(clientCtx.getSaslParams()).andReturn(null).anyTimes();

    replay(clientCtx);

    ThriftTransportKey ttk = new ThriftTransportKey(ThriftClientTypes.CLIENT,
        HostAndPort.fromParts("localhost", 9999), 120_000, clientCtx);

    assertEquals(ttk, ttk, "Normal ThriftTransportKey doesn't equal itself");
    assertEquals(ttk.hashCode(), ttk.hashCode());
  }
}
