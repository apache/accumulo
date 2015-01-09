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
import java.net.Socket;
import java.nio.channels.SelectionKey;
import java.util.Iterator;

import org.apache.log4j.Logger;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TNonblockingTransport;
import org.apache.thrift.transport.TTransportException;

/**
 * This class implements a custom non-blocking thrift server, incorporating the {@link THsHaServer} features, and overriding the underlying
 * {@link TNonblockingServer} methods, especially {@link org.apache.thrift.server.TNonblockingServer.SelectAcceptThread}, in order to override the
 * {@link org.apache.thrift.server.AbstractNonblockingServer.FrameBuffer} and {@link org.apache.thrift.server.AbstractNonblockingServer.AsyncFrameBuffer} with
 * one that reveals the client address from its transport.
 *
 * <p>
 * The justification for this is explained in https://issues.apache.org/jira/browse/ACCUMULO-1691, and is needed due to the repeated regressions:
 * <ul>
 * <li>https://issues.apache.org/jira/browse/THRIFT-958</li>
 * <li>https://issues.apache.org/jira/browse/THRIFT-1464</li>
 * <li>https://issues.apache.org/jira/browse/THRIFT-2173</li>
 * </ul>
 *
 * <p>
 * This class contains a copy of {@link org.apache.thrift.server.TNonblockingServer.SelectAcceptThread} from Thrift 0.9.1, with the slight modification of
 * instantiating a custom FrameBuffer, rather than the {@link org.apache.thrift.server.AbstractNonblockingServer.FrameBuffer} and
 * {@link org.apache.thrift.server.AbstractNonblockingServer.AsyncFrameBuffer}. Because of this, any change in the implementation upstream will require a review
 * of this implementation here, to ensure any new bugfixes/features in the upstream Thrift class are also applied here, at least until
 * https://issues.apache.org/jira/browse/THRIFT-2173 is implemented. In the meantime, the maven-enforcer-plugin ensures that Thrift remains at version 0.9.1,
 * which has been reviewed and tested.
 */
public class CustomNonBlockingServer extends THsHaServer {

  private static final Logger LOGGER = Logger.getLogger(CustomNonBlockingServer.class);
  private SelectAcceptThread selectAcceptThread_;
  private volatile boolean stopped_ = false;

  public CustomNonBlockingServer(Args args) {
    super(args);
  }

  @Override
  protected Runnable getRunnable(final FrameBuffer frameBuffer) {
    return new Runnable() {
      @Override
      public void run() {
        if (frameBuffer instanceof CustomNonblockingFrameBuffer) {
          TNonblockingTransport trans = ((CustomNonblockingFrameBuffer) frameBuffer).getTransport();
          if (trans instanceof TNonblockingSocket) {
            TNonblockingSocket tsock = (TNonblockingSocket) trans;
            Socket sock = tsock.getSocketChannel().socket();
            TServerUtils.clientAddress.set(sock.getInetAddress().getHostAddress() + ":" + sock.getPort());
          }
        }
        frameBuffer.invoke();
      }
    };
  }

  @Override
  protected boolean startThreads() {
    // start the selector
    try {
      selectAcceptThread_ = new SelectAcceptThread((TNonblockingServerTransport) serverTransport_);
      selectAcceptThread_.start();
      return true;
    } catch (IOException e) {
      LOGGER.error("Failed to start selector thread!", e);
      return false;
    }
  }

  @Override
  public void stop() {
    stopped_ = true;
    if (selectAcceptThread_ != null) {
      selectAcceptThread_.wakeupSelector();
    }
  }

  @Override
  public boolean isStopped() {
    return selectAcceptThread_.isStopped();
  }

  @Override
  protected void joinSelector() {
    // wait until the selector thread exits
    try {
      selectAcceptThread_.join();
    } catch (InterruptedException e) {
      // for now, just silently ignore. technically this means we'll have less of
      // a graceful shutdown as a result.
    }
  }

  private interface CustomNonblockingFrameBuffer {
    TNonblockingTransport getTransport();
  }

  private class CustomAsyncFrameBuffer extends AsyncFrameBuffer implements CustomNonblockingFrameBuffer {
    private TNonblockingTransport trans;

    public CustomAsyncFrameBuffer(TNonblockingTransport trans, SelectionKey selectionKey, AbstractSelectThread selectThread) {
      super(trans, selectionKey, selectThread);
      this.trans = trans;
    }

    @Override
    public TNonblockingTransport getTransport() {
      return trans;
    }
  }

  private class CustomFrameBuffer extends FrameBuffer implements CustomNonblockingFrameBuffer {
    private TNonblockingTransport trans;

    public CustomFrameBuffer(TNonblockingTransport trans, SelectionKey selectionKey, AbstractSelectThread selectThread) {
      super(trans, selectionKey, selectThread);
      this.trans = trans;
    }

    @Override
    public TNonblockingTransport getTransport() {
      return trans;
    }
  }

  // @formatter:off
  private class SelectAcceptThread extends AbstractSelectThread {

    // The server transport on which new client transports will be accepted
    private final TNonblockingServerTransport serverTransport;

    /**
     * Set up the thread that will handle the non-blocking accepts, reads, and
     * writes.
     */
    public SelectAcceptThread(final TNonblockingServerTransport serverTransport)
    throws IOException {
      this.serverTransport = serverTransport;
      serverTransport.registerSelector(selector);
    }

    public boolean isStopped() {
      return stopped_;
    }

    /**
     * The work loop. Handles both selecting (all IO operations) and managing
     * the selection preferences of all existing connections.
     */
    @Override
    public void run() {
      try {
        if (eventHandler_ != null) {
          eventHandler_.preServe();
        }

        while (!stopped_) {
          select();
          processInterestChanges();
        }
        for (SelectionKey selectionKey : selector.keys()) {
          cleanupSelectionKey(selectionKey);
        }
      } catch (Throwable t) {
        LOGGER.error("run() exiting due to uncaught error", t);
      } finally {
        stopped_ = true;
      }
    }

    /**
     * Select and process IO events appropriately:
     * If there are connections to be accepted, accept them.
     * If there are existing connections with data waiting to be read, read it,
     * buffering until a whole frame has been read.
     * If there are any pending responses, buffer them until their target client
     * is available, and then send the data.
     */
    private void select() {
      try {
        // wait for io events.
        selector.select();

        // process the io events we received
        Iterator<SelectionKey> selectedKeys = selector.selectedKeys().iterator();
        while (!stopped_ && selectedKeys.hasNext()) {
          SelectionKey key = selectedKeys.next();
          selectedKeys.remove();

          // skip if not valid
          if (!key.isValid()) {
            cleanupSelectionKey(key);
            continue;
          }

          // if the key is marked Accept, then it has to be the server
          // transport.
          if (key.isAcceptable()) {
            handleAccept();
          } else if (key.isReadable()) {
            // deal with reads
            handleRead(key);
          } else if (key.isWritable()) {
            // deal with writes
            handleWrite(key);
          } else {
            LOGGER.warn("Unexpected state in select! " + key.interestOps());
          }
        }
      } catch (IOException e) {
        LOGGER.warn("Got an IOException while selecting!", e);
      }
    }

    /**
     * Accept a new connection.
     */
    @SuppressWarnings("unused")
    private void handleAccept() throws IOException {
      SelectionKey clientKey = null;
      TNonblockingTransport client = null;
      try {
        // accept the connection
        client = (TNonblockingTransport)serverTransport.accept();
        clientKey = client.registerSelector(selector, SelectionKey.OP_READ);

        // add this key to the map
          FrameBuffer frameBuffer =
              processorFactory_.isAsyncProcessor() ? new CustomAsyncFrameBuffer(client, clientKey,SelectAcceptThread.this) :
                  new CustomFrameBuffer(client, clientKey,SelectAcceptThread.this);

          clientKey.attach(frameBuffer);
      } catch (TTransportException tte) {
        // something went wrong accepting.
        LOGGER.warn("Exception trying to accept!", tte);
        tte.printStackTrace();
        if (clientKey != null) cleanupSelectionKey(clientKey);
        if (client != null) client.close();
      }
    }
  } // SelectAcceptThread
  // @formatter:on
}
