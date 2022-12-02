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
package org.apache.accumulo.core.rpc;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.channels.spi.SelectorProvider;

import org.apache.accumulo.core.util.HostAndPort;
import org.apache.hadoop.net.NetUtils;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A utility class for setting up a {@link TTransport} with various necessary configurations for
 * ideal performance in Accumulo. These configurations include:
 * <ul>
 * <li>Setting SO_LINGER=false on the socket.</li>
 * <li>Setting TCP_NO_DELAY=true on the socket.</li>
 * <li>Setting timeouts on the I/OStreams.</li>
 * </ul>
 */
public class TTimeoutTransport {
  private static final Logger log = LoggerFactory.getLogger(TTimeoutTransport.class);

  private static final TTimeoutTransport INSTANCE = new TTimeoutTransport();

  private TTimeoutTransport() {}

  /**
   * Creates a Thrift TTransport to the given address with the given timeout. All created resources
   * are closed if an exception is thrown.
   *
   * @param addr The address to connect the client to
   * @param timeoutMillis The timeout in milliseconds for the connection
   * @return A TTransport connected to the given <code>addr</code>
   * @throws TTransportException If the transport fails to be created/connected
   */
  public static TTransport create(HostAndPort addr, long timeoutMillis) throws TTransportException {
    return INSTANCE.createInternal(new InetSocketAddress(addr.getHost(), addr.getPort()),
        timeoutMillis);
  }

  /**
   * Opens a socket to the given <code>addr</code>, configures the socket, and then creates a Thrift
   * transport using the socket.
   *
   * @param addr The address the socket should connect
   * @param timeoutMillis The socket timeout in milliseconds
   * @return A TTransport instance to the given <code>addr</code>
   * @throws TTransportException If the Thrift client is failed to be connected/created
   */
  TTransport createInternal(SocketAddress addr, long timeoutMillis) throws TTransportException {
    Socket socket = null;
    try {
      socket = openSocket(addr, (int) timeoutMillis);
    } catch (IOException e) {
      // openSocket handles closing the Socket on error
      ThriftUtil.checkIOExceptionCause(e);
      throw new TTransportException(e);
    }

    // Should be non-null
    assert socket != null;

    // Set up the streams
    try {
      InputStream input = wrapInputStream(socket, timeoutMillis);
      OutputStream output = wrapOutputStream(socket, timeoutMillis);
      return new TIOStreamTransport(input, output);
    } catch (IOException e) {
      closeSocket(socket, e);
      ThriftUtil.checkIOExceptionCause(e);
      throw new TTransportException(e);
    } catch (TTransportException e) {
      closeSocket(socket, e);
      throw e;
    }
  }

  private void closeSocket(Socket socket, Exception e) {
    try {
      if (socket != null) {
        socket.close();
      }
    } catch (IOException ioe) {
      e.addSuppressed(ioe);
      log.error("Failed to close socket after unsuccessful I/O stream setup", e);
    }
  }

  // Visible for testing
  InputStream wrapInputStream(Socket socket, long timeoutMillis) throws IOException {
    return new BufferedInputStream(NetUtils.getInputStream(socket, timeoutMillis), 1024 * 10);
  }

  // Visible for testing
  OutputStream wrapOutputStream(Socket socket, long timeoutMillis) throws IOException {
    return new BufferedOutputStream(NetUtils.getOutputStream(socket, timeoutMillis), 1024 * 10);
  }

  /**
   * Opens and configures a {@link Socket} for Accumulo RPC.
   *
   * @param addr The address to connect the socket to
   * @param timeoutMillis The timeout in milliseconds to apply to the socket connect call
   * @return A socket connected to the given address, or null if the socket fails to connect
   */
  Socket openSocket(SocketAddress addr, int timeoutMillis) throws IOException {
    Socket socket = null;
    try {
      socket = openSocketChannel();
      socket.setSoLinger(false, 0);
      socket.setTcpNoDelay(true);
      socket.connect(addr, timeoutMillis);
      return socket;
    } catch (IOException e) {
      closeSocket(socket, e);
      throw e;
    }
  }

  /**
   * Opens a socket channel and returns the underlying socket.
   */
  Socket openSocketChannel() throws IOException {
    return SelectorProvider.provider().openSocketChannel().socket();
  }
}
