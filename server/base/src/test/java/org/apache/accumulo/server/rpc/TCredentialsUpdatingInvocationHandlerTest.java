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
package org.apache.accumulo.server.rpc;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.accumulo.core.client.impl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.thrift.TCredentials;
import org.apache.accumulo.server.thrift.UGIAssumingProcessor;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TCredentialsUpdatingInvocationHandlerTest {

  TCredentialsUpdatingInvocationHandler<Object> proxy;

  @Before
  public void setup() {
    proxy = new TCredentialsUpdatingInvocationHandler<Object>(new Object());
  }

  @After
  public void teardown() {
    UGIAssumingProcessor.principal.set(null);
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
    TCredentials tcreds = new TCredentials(principal, KerberosToken.CLASS_NAME, ByteBuffer.allocate(0), UUID.randomUUID().toString());
    UGIAssumingProcessor.principal.set(principal);
    proxy.updateArgs(new Object[] {new Object(), tcreds});
    Assert.assertEquals(1, cache.size());
    Assert.assertEquals(KerberosToken.class, cache.get(KerberosToken.CLASS_NAME));
  }

  @Test(expected = ThriftSecurityException.class)
  public void testMissingPrincipal() throws Exception {
    final String principal = "root";
    TCredentials tcreds = new TCredentials(principal, KerberosToken.CLASS_NAME, ByteBuffer.allocate(0), UUID.randomUUID().toString());
    UGIAssumingProcessor.principal.set(null);
    proxy.updateArgs(new Object[] {new Object(), tcreds});
  }

  @Test(expected = ThriftSecurityException.class)
  public void testMismatchedPrincipal() throws Exception {
    final String principal = "root";
    TCredentials tcreds = new TCredentials(principal, KerberosToken.CLASS_NAME, ByteBuffer.allocate(0), UUID.randomUUID().toString());
    UGIAssumingProcessor.principal.set(principal + "foobar");
    proxy.updateArgs(new Object[] {new Object(), tcreds});
  }

  @Test(expected = ThriftSecurityException.class)
  public void testWrongTokenType() throws Exception {
    final String principal = "root";
    TCredentials tcreds = new TCredentials(principal, PasswordToken.class.getName(), ByteBuffer.allocate(0), UUID.randomUUID().toString());
    UGIAssumingProcessor.principal.set(principal);
    proxy.updateArgs(new Object[] {new Object(), tcreds});
  }
}
