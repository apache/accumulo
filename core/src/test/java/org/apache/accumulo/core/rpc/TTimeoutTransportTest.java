/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.core.rpc;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.createMockBuilder;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.net.SocketAddress;

import org.junit.Test;

/**
 * Tests for {@link TTimeoutTransport}.
 */
public class TTimeoutTransportTest {

  void expectedSocketSetup(Socket s) throws IOException {
    s.setSoLinger(false, 0);
    expectLastCall().once();
    s.setTcpNoDelay(true);
    expectLastCall().once();
  }

  @Test
  public void testFailedSocketOpenIsClosed() throws IOException {
    SocketAddress addr = createMock(SocketAddress.class);
    Socket s = createMock(Socket.class);
    TTimeoutTransport timeoutTransport = createMockBuilder(TTimeoutTransport.class).addMockedMethod("openSocketChannel").createMock();

    // Return out mocked socket
    expect(timeoutTransport.openSocketChannel()).andReturn(s).once();

    // tcpnodelay and solinger
    expectedSocketSetup(s);

    // Connect to the addr
    s.connect(addr);
    expectLastCall().andThrow(new IOException());

    // The socket should be closed after the above IOException
    s.close();

    replay(addr, s, timeoutTransport);

    try {
      timeoutTransport.openSocket(addr);
      fail("Expected to catch IOException but got none");
    } catch (IOException e) {
      // Expected
    }

    verify(addr, s, timeoutTransport);
  }

  @Test
  public void testFailedInputStreamClosesSocket() throws IOException {
    long timeout = 2 * 60 * 1000; // 2 mins
    SocketAddress addr = createMock(SocketAddress.class);
    Socket s = createMock(Socket.class);
    TTimeoutTransport timeoutTransport = createMockBuilder(TTimeoutTransport.class).addMockedMethod("openSocketChannel").addMockedMethod("wrapInputStream")
        .createMock();

    // Return out mocked socket
    expect(timeoutTransport.openSocketChannel()).andReturn(s).once();

    // tcpnodelay and solinger
    expectedSocketSetup(s);

    // Connect to the addr
    s.connect(addr);
    expectLastCall().once();

    expect(timeoutTransport.wrapInputStream(s, timeout)).andThrow(new IOException());

    // The socket should be closed after the above IOException
    s.close();

    replay(addr, s, timeoutTransport);

    try {
      timeoutTransport.createInternal(addr, timeout);
      fail("Expected to catch IOException but got none");
    } catch (IOException e) {
      // Expected
    }

    verify(addr, s, timeoutTransport);
  }

  @Test
  public void testFailedOutputStreamClosesSocket() throws IOException {
    long timeout = 2 * 60 * 1000; // 2 mins
    SocketAddress addr = createMock(SocketAddress.class);
    Socket s = createMock(Socket.class);
    InputStream is = createMock(InputStream.class);
    TTimeoutTransport timeoutTransport = createMockBuilder(TTimeoutTransport.class).addMockedMethod("openSocketChannel").addMockedMethod("wrapInputStream")
        .addMockedMethod("wrapOutputStream").createMock();

    // Return out mocked socket
    expect(timeoutTransport.openSocketChannel()).andReturn(s).once();

    // tcpnodelay and solinger
    expectedSocketSetup(s);

    // Connect to the addr
    s.connect(addr);
    expectLastCall().once();

    // Input stream is set up
    expect(timeoutTransport.wrapInputStream(s, timeout)).andReturn(is);
    // Output stream fails to be set up
    expect(timeoutTransport.wrapOutputStream(s, timeout)).andThrow(new IOException());

    // The socket should be closed after the above IOException
    s.close();

    replay(addr, s, timeoutTransport);

    try {
      timeoutTransport.createInternal(addr, timeout);
      fail("Expected to catch IOException but got none");
    } catch (IOException e) {
      // Expected
    }

    verify(addr, s, timeoutTransport);
  }

}
