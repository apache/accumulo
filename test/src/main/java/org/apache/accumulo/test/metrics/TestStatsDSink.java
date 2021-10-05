/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.test.metrics;

import java.io.Closeable;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.accumulo.core.util.threads.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestStatsDSink implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(TestStatsDSink.class);

  private final DatagramSocket sock;
  private final ConcurrentLinkedQueue<String> received = new ConcurrentLinkedQueue<>();

  public TestStatsDSink() throws SocketException {
    sock = new DatagramSocket();
    int len = sock.getReceiveBufferSize();
    Threads.createThread("test-server-thread", () -> {
      while (!sock.isClosed()) {
        byte[] buf = new byte[len];
        DatagramPacket packet = new DatagramPacket(buf, len);
        try {
          sock.receive(packet);
          received.add(new String(packet.getData()));
        } catch (IOException e) {
          if (!sock.isClosed()) {
            LOG.error("Error receiving packet", e);
            sock.close();
          }
        }
      }

    }).start();
  }

  public String getHost() {
    return sock.getLocalAddress().getHostAddress();
  }

  public int getPort() {
    return sock.getLocalPort();
  }

  public List<String> getLines() {
    return new ArrayList<String>(received);
  }

  public String getLine() {
    return received.remove();
  }

  @Override
  public void close() {
    sock.close();
  }
}
