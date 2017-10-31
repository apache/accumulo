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
package org.apache.accumulo.core.rpc;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.channels.spi.SelectorProvider;

import org.apache.accumulo.core.util.HostAndPort;
import org.apache.hadoop.net.NetUtils;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A utility class for setting up a {@link TTransport} with various necessary configurations for ideal performance in Accumulo. These configurations include:
 * <ul>
 * <li>Setting SO_LINGER=false on the socket.</li>
 * <li>Setting TCP_NO_DELAY=true on the socket.</li>
 * <li>Setting timeouts on the I/OStreams.</li>
 * </ul>
 */
public class TTimeoutTransport {
  private static final Logger log = LoggerFactory.getLogger(TTimeoutTransport.class);

  private static final TTimeoutTransport INSTANCE = new TTimeoutTransport();

  private volatile Method GET_INPUT_STREAM_METHOD = null;

  private TTimeoutTransport() {}

  private Method getNetUtilsInputStreamMethod() {
    if (null == GET_INPUT_STREAM_METHOD) {
      synchronized (this) {
        if (null == GET_INPUT_STREAM_METHOD) {
          try {
            GET_INPUT_STREAM_METHOD = NetUtils.class.getMethod("getInputStream", Socket.class, Long.TYPE);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      }
    }

    return GET_INPUT_STREAM_METHOD;
  }

  /**
   * Invokes the <code>NetUtils.getInputStream(Socket, long)</code> using reflection to handle compatibility with both Hadoop 1 and 2.
   *
   * @param socket
   *          The socket to create the input stream on
   * @param timeout
   *          The timeout for the input stream in milliseconds
   * @return An InputStream on the socket
   */
  private InputStream getInputStream(Socket socket, long timeout) throws IOException {
    try {
      return (InputStream) getNetUtilsInputStreamMethod().invoke(null, socket, timeout);
    } catch (Exception e) {
      Throwable cause = e.getCause();
      // Try to re-throw the IOException directly
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }

      if (e instanceof RuntimeException) {
        // Don't re-wrap another RTE around an RTE
        throw (RuntimeException) e;
      } else {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Creates a Thrift TTransport to the given address with the given timeout. All created resources are closed if an exception is thrown.
   *
   * @param addr
   *          The address to connect the client to
   * @param timeoutMillis
   *          The timeout in milliseconds for the connection
   * @return A TTransport connected to the given <code>addr</code>
   * @throws IOException
   *           If the transport fails to be created/connected
   */
  public static TTransport create(HostAndPort addr, long timeoutMillis) throws IOException {
    return INSTANCE.createInternal(new InetSocketAddress(addr.getHost(), addr.getPort()), timeoutMillis);
  }

  /**
   * Creates a Thrift TTransport to the given address with the given timeout. All created resources are closed if an exception is thrown.
   *
   * @param addr
   *          The address to connect the client to
   * @param timeoutMillis
   *          The timeout in milliseconds for the connection
   * @return A TTransport connected to the given <code>addr</code>
   * @throws IOException
   *           If the transport fails to be created/connected
   */
  public static TTransport create(SocketAddress addr, long timeoutMillis) throws IOException {
    return INSTANCE.createInternal(addr, timeoutMillis);
  }

  /**
   * Opens a socket to the given <code>addr</code>, configures the socket, and then creates a Thrift transport using the socket.
   *
   * @param addr
   *          The address the socket should connect
   * @param timeoutMillis
   *          The socket timeout in milliseconds
   * @return A TTransport instance to the given <code>addr</code>
   * @throws IOException
   *           If the Thrift client is failed to be connected/created
   */
  protected TTransport createInternal(SocketAddress addr, long timeoutMillis) throws IOException {
    Socket socket = null;
    try {
      socket = openSocket(addr);
    } catch (IOException e) {
      // openSocket handles closing the Socket on error
      throw e;
    }

    // Should be non-null
    assert null != socket;

    // Set up the streams
    try {
      InputStream input = wrapInputStream(socket, timeoutMillis);
      OutputStream output = wrapOutputStream(socket, timeoutMillis);
      return new TIOStreamTransport(input, output);
    } catch (IOException e) {
      try {
        socket.close();
      } catch (IOException ioe) {
        log.error("Failed to close socket after unsuccessful I/O stream setup", e);
      }

      throw e;
    }
  }

  // Visible for testing
  protected InputStream wrapInputStream(Socket socket, long timeoutMillis) throws IOException {
    return new BufferedInputStream(getInputStream(socket, timeoutMillis), 1024 * 10);
  }

  // Visible for testing
  protected OutputStream wrapOutputStream(Socket socket, long timeoutMillis) throws IOException {
    return new BufferedOutputStream(NetUtils.getOutputStream(socket, timeoutMillis), 1024 * 10);
  }

  /**
   * Opens and configures a {@link Socket} for Accumulo RPC.
   *
   * @param addr
   *          The address to connect the socket to
   * @return A socket connected to the given address, or null if the socket fails to connect
   */
  protected Socket openSocket(SocketAddress addr) throws IOException {
    Socket socket = null;
    try {
      socket = openSocketChannel();
      socket.setSoLinger(false, 0);
      socket.setTcpNoDelay(true);
      socket.connect(addr);
      return socket;
    } catch (IOException e) {
      try {
        if (socket != null)
          socket.close();
      } catch (IOException ioe) {
        log.error("Failed to close socket after unsuccessful open.", e);
      }

      throw e;
    }
  }

  /**
   * Opens a socket channel and returns the underlying socket.
   */
  protected Socket openSocketChannel() throws IOException {
    return SelectorProvider.provider().openSocketChannel().socket();
  }
}
