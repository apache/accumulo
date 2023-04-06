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
package org.apache.accumulo.test.rpc;

import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.thrift.TConfiguration;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransport;

/**
 * Mocket - a Mock Socket
 * <p>
 * Implements a bi-directional client-server transport in memory, using two FIFO queues. The output
 * stream of the client is wired to the input stream of the server, and the output stream of the
 * server is wired to the input stream of the client.
 */
public class Mocket {

  private final TTransport clientTransport;
  private final TServerTransport serverTransport;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  public Mocket() {
    Buffer serverQueue = new Buffer();
    Buffer clientQueue = new Buffer();
    // wire up the two queues to each other
    clientTransport = new MocketTransport(clientQueue, serverQueue);
    serverTransport = new MocketServerTransport(new MocketTransport(serverQueue, clientQueue));

  }

  public TServerTransport getServerTransport() {
    return serverTransport;
  }

  public TTransport getClientTransport() {
    return clientTransport;
  }

  private boolean isMocketClosed() {
    return closed.get();
  }

  private void closeMocket() {
    closed.set(true);
  }

  private class Buffer {

    private final LinkedBlockingQueue<Integer> queue = new LinkedBlockingQueue<>();

    public void write(int b) {
      queue.add(b);
    }

    public void write(byte[] buf, int off, int len) {
      Objects.requireNonNull(buf);
      Objects.checkFromToIndex(off, off + len, buf.length);
      if (len == 0) {
        return;
      }
      for (int i = 0; i < len; i++) {
        write(buf[off + i]);
      }
    }

    public int read() {
      Integer item;
      // item = queue.take();
      // loop below makes sure we don't block indefinitely
      while (!isMocketClosed()) {
        try {
          item = queue.poll(10, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          // reset interrupt flag before returning
          Thread.currentThread().interrupt();
          closeMocket();
          return -1;
        }
        // null means the timeout was reached
        if (item != null) {
          return item;
        }
      }
      return -1;
    }

    public int read(byte[] buf, int off, int len) {
      Objects.requireNonNull(buf);
      Objects.checkFromToIndex(off, off + len, buf.length);
      if (len == 0) {
        return 0;
      }
      int c = read();
      if (c == -1) {
        return -1;
      }
      buf[off] = (byte) c;

      int i;
      for (i = 1; i < len; i++) {
        c = read();
        if (c == -1) {
          break;
        }
        buf[off + i] = (byte) c;
      }
      return i;
    }

  }

  private static class MocketServerTransport extends TServerTransport {

    private final MocketTransport servTrans;

    public MocketServerTransport(MocketTransport mocketTransport) {
      servTrans = mocketTransport;
    }

    @Override
    public void listen() {}

    @Override
    public TTransport accept() {
      return servTrans;
    }

    @Override
    public void close() {
      servTrans.close();
    }

    @Override
    public void interrupt() {
      close();
    }

  }

  private class MocketTransport extends TTransport {

    private final Buffer input;
    private final Buffer output;

    private MocketTransport(Buffer input, Buffer output) {
      this.input = input;
      this.output = output;
    }

    @Override
    public void write(byte[] buf, int off, int len) {
      output.write(buf, off, len);
    }

    @Override
    public TConfiguration getConfiguration() {
      return null;
    }

    @Override
    public void updateKnownMessageSize(long size) {

    }

    @Override
    public void checkReadBytesAvailable(long numBytes) {

    }

    @Override
    public int read(byte[] buf, int off, int len) {
      return input.read(buf, off, len);
    }

    @Override
    public void open() {}

    @Override
    public boolean isOpen() {
      return !isMocketClosed();
    }

    @Override
    public void close() {
      closeMocket();
    }
  }

}
