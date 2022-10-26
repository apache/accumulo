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

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.Socket;
import java.nio.channels.SelectionKey;

import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TNonblockingTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements a custom non-blocking thrift server that stores the client address in
 * thread-local storage for the invocation.
 */
public class CustomNonBlockingServer extends THsHaServer {

  private static final Logger log = LoggerFactory.getLogger(CustomNonBlockingServer.class);
  private final Field selectAcceptThreadField;

  public CustomNonBlockingServer(Args args) {
    super(args);

    try {
      selectAcceptThreadField = TNonblockingServer.class.getDeclaredField("selectAcceptThread_");
      selectAcceptThreadField.setAccessible(true);
    } catch (Exception e) {
      throw new RuntimeException("Failed to access required field in Thrift code.", e);
    }
  }

  @Override
  public void stop() {
    super.stop();
    try {
      getInvoker().shutdownNow();
    } catch (Exception e) {
      log.error("Unable to call shutdownNow", e);
    }
  }

  @Override
  protected boolean startThreads() {
    // Yet another dirty/gross hack to get access to the client's address.

    // start the selector
    try {
      // Hack in our SelectAcceptThread impl
      SelectAcceptThread selectAcceptThread_ =
          new CustomSelectAcceptThread((TNonblockingServerTransport) serverTransport_);
      // Set the private field before continuing.
      selectAcceptThreadField.set(this, selectAcceptThread_);

      selectAcceptThread_.start();
      return true;
    } catch (IOException e) {
      LOGGER.error("Failed to start selector thread!", e);
      return false;
    } catch (IllegalAccessException | IllegalArgumentException e) {
      throw new RuntimeException("Exception setting customer select thread in Thrift");
    }
  }

  /**
   * Custom wrapper around {@link org.apache.thrift.server.TNonblockingServer.SelectAcceptThread} to
   * create our {@link CustomFrameBuffer}.
   */
  private class CustomSelectAcceptThread extends SelectAcceptThread {

    public CustomSelectAcceptThread(TNonblockingServerTransport serverTransport)
        throws IOException {
      super(serverTransport);
    }

    @Override
    protected FrameBuffer createFrameBuffer(final TNonblockingTransport trans,
        final SelectionKey selectionKey, final AbstractSelectThread selectThread)
        throws TTransportException {
      if (processorFactory_.isAsyncProcessor()) {
        throw new IllegalStateException("This implementation does not support AsyncProcessors");
      }

      return new CustomFrameBuffer(trans, selectionKey, selectThread);
    }
  }

  /**
   * Custom wrapper around {@link org.apache.thrift.server.AbstractNonblockingServer.FrameBuffer} to
   * extract the client's network location before accepting the request.
   */
  private class CustomFrameBuffer extends FrameBuffer {

    public CustomFrameBuffer(TNonblockingTransport trans, SelectionKey selectionKey,
        AbstractSelectThread selectThread) throws TTransportException {
      super(trans, selectionKey, selectThread);
    }

    @Override
    public void invoke() {
      if (trans_ instanceof TNonblockingSocket) {
        TNonblockingSocket tsock = (TNonblockingSocket) trans_;
        Socket sock = tsock.getSocketChannel().socket();
        TServerUtils.clientAddress
            .set(sock.getInetAddress().getHostAddress() + ":" + sock.getPort());
      }
      super.invoke();
    }

    @Override
    public boolean read() {
      boolean result = super.read();
      if (!result) {
        log.trace("CustomFrameBuffer.read returned false when reading data from client: {}",
            TServerUtils.clientAddress.get());
      }
      return result;
    }

    @Override
    public boolean write() {
      boolean result = super.write();
      if (!result) {
        log.trace("CustomFrameBuffer.write returned false when writing data to client: {}",
            TServerUtils.clientAddress.get());
      }
      return result;
    }

  }

}
