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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import org.apache.accumulo.core.rpc.SaslConnectionParams;
import org.apache.accumulo.core.rpc.SslConnectionParams;
import org.junit.Test;

import com.google.common.net.HostAndPort;

public class ThriftTransportKeyTest {

  @Test(expected = RuntimeException.class)
  public void testSslAndSaslErrors() {
    ClientContext clientCtx = createMock(ClientContext.class);
    SslConnectionParams sslParams = createMock(SslConnectionParams.class);
    SaslConnectionParams saslParams = createMock(SaslConnectionParams.class);

    expect(clientCtx.getClientSslParams()).andReturn(sslParams).anyTimes();
    expect(clientCtx.getClientSaslParams()).andReturn(saslParams).anyTimes();

    // We don't care to verify the sslparam or saslparam mocks
    replay(clientCtx);

    try {
      new ThriftTransportKey(HostAndPort.fromParts("localhost", 9999), 120 * 1000, clientCtx);
    } finally {
      verify(clientCtx);
    }
  }

  @Test
  public void testSaslPrincipalIsSignificant() {
    SaslConnectionParams saslParams1 = createMock(SaslConnectionParams.class), saslParams2 = createMock(SaslConnectionParams.class);
    expect(saslParams1.getPrincipal()).andReturn("user1");
    expect(saslParams2.getPrincipal()).andReturn("user2");

    replay(saslParams1, saslParams2);

    ThriftTransportKey ttk1 = new ThriftTransportKey(HostAndPort.fromParts("localhost", 9997), 1l, null, saslParams1), ttk2 = new ThriftTransportKey(
        HostAndPort.fromParts("localhost", 9997), 1l, null, saslParams2);

    assertNotEquals(ttk1, ttk2);
    assertNotEquals(ttk1.hashCode(), ttk2.hashCode());

    verify(saslParams1, saslParams2);
  }

  @Test
  public void testSimpleEquivalence() {
    ClientContext clientCtx = createMock(ClientContext.class);

    expect(clientCtx.getClientSslParams()).andReturn(null).anyTimes();
    expect(clientCtx.getClientSaslParams()).andReturn(null).anyTimes();

    replay(clientCtx);

    ThriftTransportKey ttk = new ThriftTransportKey(HostAndPort.fromParts("localhost", 9999), 120 * 1000, clientCtx);

    assertTrue("Normal ThriftTransportKey doesn't equal itself", ttk.equals(ttk));
  }

}
