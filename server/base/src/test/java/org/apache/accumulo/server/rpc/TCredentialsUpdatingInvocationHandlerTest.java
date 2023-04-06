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
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TCredentialsUpdatingInvocationHandlerTest {

  private TCredentialsUpdatingInvocationHandler<Object> proxy;
  private ConfigurationCopy conf;

  @BeforeEach
  public void setup() {
    conf = new ConfigurationCopy(DefaultConfiguration.getInstance());
    proxy = new TCredentialsUpdatingInvocationHandler<>(new Object(), conf);
  }

  @AfterEach
  public void teardown() {
    UGIAssumingProcessor.rpcPrincipal.set(null);
  }

  @Test
  public void testNoArgsAreIgnored() throws Exception {
    proxy.updateArgs(new Object[] {});
  }

  @Test
  public void testNoTCredsInArgsAreIgnored() throws Exception {
    proxy.updateArgs(new Object[] {new Object(), new Object()});
  }

  @Test
  public void testCachedTokenClass() throws Exception {
    final String principal = "root";
    ConcurrentHashMap<String,Class<? extends AuthenticationToken>> cache = proxy.getTokenCache();
    cache.clear();
    TCredentials tcreds = new TCredentials(principal, KerberosToken.CLASS_NAME,
        ByteBuffer.allocate(0), UUID.randomUUID().toString());
    UGIAssumingProcessor.rpcPrincipal.set(principal);
    proxy.updateArgs(new Object[] {new Object(), tcreds});
    assertEquals(1, cache.size());
    assertEquals(KerberosToken.class, cache.get(KerberosToken.CLASS_NAME));
  }

  @Test
  public void testMissingPrincipal() {
    final String principal = "root";
    TCredentials tcreds = new TCredentials(principal, KerberosToken.CLASS_NAME,
        ByteBuffer.allocate(0), UUID.randomUUID().toString());
    UGIAssumingProcessor.rpcPrincipal.set(null);
    assertThrows(ThriftSecurityException.class,
        () -> proxy.updateArgs(new Object[] {new Object(), tcreds}));
  }

  @Test
  public void testMismatchedPrincipal() {
    final String principal = "root";
    TCredentials tcreds = new TCredentials(principal, KerberosToken.CLASS_NAME,
        ByteBuffer.allocate(0), UUID.randomUUID().toString());
    UGIAssumingProcessor.rpcPrincipal.set(principal + "foobar");
    assertThrows(ThriftSecurityException.class,
        () -> proxy.updateArgs(new Object[] {new Object(), tcreds}));
  }

  @Test
  public void testWrongTokenType() {
    final String principal = "root";
    TCredentials tcreds = new TCredentials(principal, PasswordToken.class.getName(),
        ByteBuffer.allocate(0), UUID.randomUUID().toString());
    UGIAssumingProcessor.rpcPrincipal.set(principal);
    assertThrows(ThriftSecurityException.class,
        () -> proxy.updateArgs(new Object[] {new Object(), tcreds}));
  }

  @Test
  public void testAllowedAnyImpersonationForAnyUser() throws Exception {
    final String proxyServer = "proxy";
    conf.set(Property.INSTANCE_RPC_SASL_ALLOWED_USER_IMPERSONATION, proxyServer + ":*");
    conf.set(Property.INSTANCE_RPC_SASL_ALLOWED_HOST_IMPERSONATION, "*");
    proxy = new TCredentialsUpdatingInvocationHandler<>(new Object(), conf);
    TCredentials tcreds = new TCredentials("client", KerberosToken.class.getName(),
        ByteBuffer.allocate(0), UUID.randomUUID().toString());
    UGIAssumingProcessor.rpcPrincipal.set(proxyServer);
    proxy.updateArgs(new Object[] {new Object(), tcreds});
  }

  @Test
  public void testAllowedImpersonationForSpecificUsers() throws Exception {
    final String proxyServer = "proxy";
    conf.set(Property.INSTANCE_RPC_SASL_ALLOWED_USER_IMPERSONATION,
        proxyServer + ":client1,client2");
    conf.set(Property.INSTANCE_RPC_SASL_ALLOWED_HOST_IMPERSONATION, "*");
    proxy = new TCredentialsUpdatingInvocationHandler<>(new Object(), conf);
    TCredentials tcreds = new TCredentials("client1", KerberosToken.class.getName(),
        ByteBuffer.allocate(0), UUID.randomUUID().toString());
    UGIAssumingProcessor.rpcPrincipal.set(proxyServer);
    proxy.updateArgs(new Object[] {new Object(), tcreds});
    tcreds = new TCredentials("client2", KerberosToken.class.getName(), ByteBuffer.allocate(0),
        UUID.randomUUID().toString());
    proxy.updateArgs(new Object[] {new Object(), tcreds});
  }

  @Test
  public void testDisallowedImpersonationForUser() {
    final String proxyServer = "proxy";
    // let "otherproxy" impersonate, but not "proxy"
    conf.set(Property.INSTANCE_RPC_SASL_ALLOWED_USER_IMPERSONATION, "otherproxy:*");
    conf.set(Property.INSTANCE_RPC_SASL_ALLOWED_HOST_IMPERSONATION, "*");
    proxy = new TCredentialsUpdatingInvocationHandler<>(new Object(), conf);
    TCredentials tcreds = new TCredentials("client", KerberosToken.class.getName(),
        ByteBuffer.allocate(0), UUID.randomUUID().toString());
    UGIAssumingProcessor.rpcPrincipal.set(proxyServer);
    assertThrows(ThriftSecurityException.class,
        () -> proxy.updateArgs(new Object[] {new Object(), tcreds}));
  }

  @Test
  public void testDisallowedImpersonationForMultipleUsers() {
    final String proxyServer = "proxy";
    // let "otherproxy" impersonate, but not "proxy"
    conf.set(Property.INSTANCE_RPC_SASL_ALLOWED_USER_IMPERSONATION,
        "otherproxy1:*;otherproxy2:client1,client2");
    conf.set(Property.INSTANCE_RPC_SASL_ALLOWED_HOST_IMPERSONATION, "*;*");
    proxy = new TCredentialsUpdatingInvocationHandler<>(new Object(), conf);
    TCredentials tcreds = new TCredentials("client1", KerberosToken.class.getName(),
        ByteBuffer.allocate(0), UUID.randomUUID().toString());
    UGIAssumingProcessor.rpcPrincipal.set(proxyServer);
    assertThrows(ThriftSecurityException.class,
        () -> proxy.updateArgs(new Object[] {new Object(), tcreds}));
  }

  @Test
  public void testAllowedImpersonationFromSpecificHost() throws Exception {
    final String proxyServer = "proxy", client = "client", host = "host.domain.com";
    conf.set(Property.INSTANCE_RPC_SASL_ALLOWED_USER_IMPERSONATION, proxyServer + ":" + client);
    conf.set(Property.INSTANCE_RPC_SASL_ALLOWED_HOST_IMPERSONATION, host);
    proxy = new TCredentialsUpdatingInvocationHandler<>(new Object(), conf);
    TCredentials tcreds = new TCredentials("client", KerberosToken.class.getName(),
        ByteBuffer.allocate(0), UUID.randomUUID().toString());
    UGIAssumingProcessor.rpcPrincipal.set(proxyServer);
    TServerUtils.clientAddress.set(host);
    proxy.updateArgs(new Object[] {new Object(), tcreds});
  }

  @Test
  public void testDisallowedImpersonationFromSpecificHost() {
    final String proxyServer = "proxy", client = "client", host = "host.domain.com";
    conf.set(Property.INSTANCE_RPC_SASL_ALLOWED_USER_IMPERSONATION, proxyServer + ":" + client);
    conf.set(Property.INSTANCE_RPC_SASL_ALLOWED_HOST_IMPERSONATION, host);
    proxy = new TCredentialsUpdatingInvocationHandler<>(new Object(), conf);
    TCredentials tcreds = new TCredentials("client", KerberosToken.class.getName(),
        ByteBuffer.allocate(0), UUID.randomUUID().toString());
    UGIAssumingProcessor.rpcPrincipal.set(proxyServer);
    // The RPC came from a different host than is allowed
    TServerUtils.clientAddress.set("otherhost.domain.com");
    assertThrows(ThriftSecurityException.class,
        () -> proxy.updateArgs(new Object[] {new Object(), tcreds}));
  }
}
