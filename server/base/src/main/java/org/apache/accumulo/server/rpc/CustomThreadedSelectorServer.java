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

import java.lang.reflect.Field;
import java.net.Socket;

import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TNonblockingTransport;
import org.slf4j.LoggerFactory;

public class CustomThreadedSelectorServer extends TThreadedSelectorServer {

  private final Field fbTansportField;

  public CustomThreadedSelectorServer(Args args) {
    super(args);

    try {
      fbTansportField = FrameBuffer.class.getDeclaredField("trans_");
      fbTansportField.setAccessible(true);
    } catch (SecurityException | NoSuchFieldException e) {
      throw new RuntimeException("Failed to access required field in Thrift code.", e);
    }
  }

  private TNonblockingTransport getTransport(FrameBuffer frameBuffer) {
    try {
      return (TNonblockingTransport) fbTansportField.get(frameBuffer);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected Runnable getRunnable(FrameBuffer frameBuffer) {
    return () -> {

      try {
        TNonblockingTransport transport = getTransport(frameBuffer);

        if (transport instanceof TNonblockingSocket) {
          // This block of code makes the client address available to the server side code that
          // executes a RPC. It is made available for informational purposes.
          TNonblockingSocket tsock = (TNonblockingSocket) transport;
          Socket sock = tsock.getSocketChannel().socket();
          TServerUtils.clientAddress
              .set(sock.getInetAddress().getHostAddress() + ":" + sock.getPort());
        }
      } catch (Exception e) {
        LoggerFactory.getLogger(CustomThreadedSelectorServer.class)
            .warn("Failed to get client address ", e);
      }
      frameBuffer.invoke();
    };
  }

}
