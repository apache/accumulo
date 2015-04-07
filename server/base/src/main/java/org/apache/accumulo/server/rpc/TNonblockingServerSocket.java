/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.accumulo.server.rpc;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper around ServerSocketChannel.
 *
 * This class is copied from org.apache.thrift.transport.TNonblockingServerSocket version 0.9. The only change (apart from the logging statements) is the
 * addition of the {@link #getPort()} method to retrieve the port used by the ServerSocket.
 */
public class TNonblockingServerSocket extends TNonblockingServerTransport {
  private static final Logger log = LoggerFactory.getLogger(TNonblockingServerTransport.class.getName());

  /**
   * This channel is where all the nonblocking magic happens.
   */
  private ServerSocketChannel serverSocketChannel = null;

  /**
   * Underlying ServerSocket object
   */
  private ServerSocket serverSocket_ = null;

  /**
   * Timeout for client sockets from accept
   */
  private int clientTimeout_ = 0;

  /**
   * Creates just a port listening server socket
   */
  public TNonblockingServerSocket(int port) throws TTransportException {
    this(port, 0);
  }

  /**
   * Creates just a port listening server socket
   */
  public TNonblockingServerSocket(int port, int clientTimeout) throws TTransportException {
    this(new InetSocketAddress(port), clientTimeout);
  }

  public TNonblockingServerSocket(InetSocketAddress bindAddr) throws TTransportException {
    this(bindAddr, 0);
  }

  public TNonblockingServerSocket(InetSocketAddress bindAddr, int clientTimeout) throws TTransportException {
    clientTimeout_ = clientTimeout;
    try {
      serverSocketChannel = ServerSocketChannel.open();
      serverSocketChannel.configureBlocking(false);

      // Make server socket
      serverSocket_ = serverSocketChannel.socket();
      // Prevent 2MSL delay problem on server restarts
      serverSocket_.setReuseAddress(true);
      // Bind to listening port
      serverSocket_.bind(bindAddr);
    } catch (IOException ioe) {
      serverSocket_ = null;
      throw new TTransportException("Could not create ServerSocket on address " + bindAddr.toString() + ".");
    }
  }

  @Override
  public void listen() throws TTransportException {
    // Make sure not to block on accept
    if (serverSocket_ != null) {
      try {
        serverSocket_.setSoTimeout(0);
      } catch (SocketException sx) {
        log.error("SocketException caused by serverSocket in listen()", sx);
      }
    }
  }

  @Override
  protected TNonblockingSocket acceptImpl() throws TTransportException {
    if (serverSocket_ == null) {
      throw new TTransportException(TTransportException.NOT_OPEN, "No underlying server socket.");
    }
    try {
      SocketChannel socketChannel = serverSocketChannel.accept();
      if (socketChannel == null) {
        return null;
      }

      TNonblockingSocket tsocket = new TNonblockingSocket(socketChannel);
      tsocket.setTimeout(clientTimeout_);
      return tsocket;
    } catch (IOException iox) {
      throw new TTransportException(iox);
    }
  }

  @Override
  public void registerSelector(Selector selector) {
    try {
      // Register the server socket channel, indicating an interest in
      // accepting new connections
      serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
    } catch (ClosedChannelException e) {
      // this shouldn't happen, ideally...
      // TODO: decide what to do with this.
    }
  }

  @Override
  public void close() {
    if (serverSocket_ != null) {
      try {
        serverSocket_.close();
      } catch (IOException iox) {
        log.warn("WARNING: Could not close server socket: " + iox.getMessage());
      }
      serverSocket_ = null;
    }
  }

  @Override
  public void interrupt() {
    // The thread-safeness of this is dubious, but Java documentation suggests
    // that it is safe to do this from a different thread context
    close();
  }

  public int getPort() {
    return serverSocket_.getLocalPort();
  }
}
