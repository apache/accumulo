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
import java.util.concurrent.atomic.AtomicLong;

import org.apache.thrift.server.AbstractNonblockingServer;
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

  private final AtomicLong readBufferBytesAllocated2;

  public CustomNonBlockingServer(Args args) {
    super(args);

    try {
      selectAcceptThreadField = TNonblockingServer.class.getDeclaredField("selectAcceptThread_");
      selectAcceptThreadField.setAccessible(true);

      Field rbbaField =
          AbstractNonblockingServer.class.getDeclaredField("readBufferBytesAllocated");
      rbbaField.setAccessible(true);
      readBufferBytesAllocated2 = (AtomicLong) rbbaField.get(this);
      Field mrbbField = AbstractNonblockingServer.class.getDeclaredField("MAX_READ_BUFFER_BYTES");
      mrbbField.setAccessible(true);
      log.debug("MAX_READ_BUFFER_BYTES={}", mrbbField.getLong(this));
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
    private final String clientAddress;

    public CustomFrameBuffer(TNonblockingTransport trans, SelectionKey selectionKey,
        AbstractSelectThread selectThread) throws TTransportException {
      super(trans, selectionKey, selectThread);
      // Store the clientAddress in the buffer so it can be referenced for logging during read/write
      this.clientAddress = getClientAddress();
    }

    @Override
    public void invoke() {
      // On invoke() set the clientAddress on the ThreadLocal so that it can be accessed elsewhere
      // in the same thread that called invoke() on the buffer
      TServerUtils.clientAddress.set(clientAddress);
      log.trace("CustomFrameBuffer.invoke before client:{} total_buffers_size:{}", clientAddress,
          readBufferBytesAllocated2.get());
      super.invoke();
      log.trace("CustomFrameBuffer.invoke after client:{} total_buffers_size:{}", clientAddress,
          readBufferBytesAllocated2.get());
    }

    @Override
    public boolean read() {
      log.trace(
          "CustomFrameBuffer.read before read client: {} buffer_size:{} state:{} total_buffers_size:{}",
          clientAddress, buffer_.array().length, state_, readBufferBytesAllocated2.get());
      boolean result = super.read();
      // if (!result) {
      log.trace(
          "CustomFrameBuffer.read returned {} when reading data from client: {} buffer_size:{} state:{} total_buffers_size:{}",
          result, clientAddress, buffer_.array().length, state_, readBufferBytesAllocated2.get());
      // }
      return result;
    }

    @Override
    public boolean write() {
      boolean result = super.write();
      // if (!result) {
      log.trace(
          "CustomFrameBuffer.write returned {} when writing data to client: {} buffer_size:{} state:{}",
          result, clientAddress, buffer_.array().length, state_);
      // }
      return result;
    }

    @Override
    public void close() {
      log.trace("CustomFrameBuffer.close before client:{} total_buffers_size:{}", clientAddress,
          readBufferBytesAllocated2.get());
      super.close();
      log.trace("CustomFrameBuffer.close after client:{} total_buffers_size:{}", clientAddress,
          readBufferBytesAllocated2.get());
    }

    /*
     * Helper method used to capture the client address inside the CustomFrameBuffer constructor so
     * that it can be referenced inside the read/write methods for logging purposes. It previously
     * was only set on the ThreadLocal in the invoke() method but that does not work because A) the
     * method isn't called until after reading is finished so the value will be null inside of
     * read() and B) The other problem is that invoke() is called on a different thread than
     * read()/write() so even if the order was correct it would not be available.
     *
     * Since a new FrameBuffer is created for each request we can use it to capture the client
     * address earlier in the constructor and not wait for invoke(). A FrameBuffer is used to read
     * data and write a response back to the client and as part of creation of the buffer the
     * TNonblockingSocket is stored as a final variable and won't change so we can safely capture
     * the clientAddress in the constructor and use it for logging during read/write and then use
     * the value inside of invoke() to set the ThreadLocal so the client address will still be
     * available on the thread that called invoke().
     */
    private String getClientAddress() {
      String clientAddress = null;
      if (trans_ instanceof TNonblockingSocket) {
        TNonblockingSocket tsock = (TNonblockingSocket) trans_;
        Socket sock = tsock.getSocketChannel().socket();
        clientAddress = sock.getInetAddress().getHostAddress() + ":" + sock.getPort();
        log.trace("CustomFrameBuffer captured client address: {}", clientAddress);
      }
      return clientAddress;
    }
  }

}
