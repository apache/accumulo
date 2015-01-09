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
package org.apache.accumulo.core.util;

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

import org.apache.hadoop.net.NetUtils;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransport;

import com.google.common.net.HostAndPort;

public class TTimeoutTransport {

  private static InputStream getInputStream(Socket socket, long timeout) {
    try {
      Method m = NetUtils.class.getMethod("getInputStream", Socket.class, Long.TYPE);
      return (InputStream) m.invoke(null, socket, timeout);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static TTransport create(HostAndPort addr, long timeoutMillis) throws IOException {
    return create(new InetSocketAddress(addr.getHostText(), addr.getPort()), timeoutMillis);
  }

  public static TTransport create(SocketAddress addr, long timeoutMillis) throws IOException {
    Socket socket = SelectorProvider.provider().openSocketChannel().socket();
    socket.setSoLinger(false, 0);
    socket.setTcpNoDelay(true);
    socket.connect(addr);
    InputStream input = new BufferedInputStream(getInputStream(socket, timeoutMillis), 1024 * 10);
    OutputStream output = new BufferedOutputStream(NetUtils.getOutputStream(socket, timeoutMillis), 1024 * 10);
    return new TIOStreamTransport(input, output);
  }
}
