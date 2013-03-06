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
package org.apache.accumulo.server.util;

import java.io.IOException;
import java.net.ServerSocket;

import org.apache.accumulo.core.util.TBufferedSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

// Thrift-959 removed the small buffer from TSocket; this adds it back for servers
public class TBufferedServerSocket extends TServerTransport {

  // expose acceptImpl
  static class TServerSocket extends org.apache.thrift.transport.TServerSocket {
    public TServerSocket(ServerSocket serverSocket) {
      super(serverSocket);
    }

    public TSocket acceptImplPublic() throws TTransportException {
      return acceptImpl();
    }
  }

  final TServerSocket impl;
  final int bufferSize;

  public TBufferedServerSocket(ServerSocket serverSocket, int bufferSize) {
    this.impl = new TServerSocket(serverSocket);
    this.bufferSize = bufferSize;
  }

  @Override
  public void listen() throws TTransportException {
    impl.listen();
  }

  @Override
  public void close() {
    impl.close();
  }

  // Wrap accepted sockets using buffered IO
  @Override
  protected TTransport acceptImpl() throws TTransportException {
    TSocket sock = impl.acceptImplPublic();
    try {
      return new TBufferedSocket(sock, this.bufferSize);
    } catch (IOException e) {
      throw new TTransportException(e);
    }
  }

}
