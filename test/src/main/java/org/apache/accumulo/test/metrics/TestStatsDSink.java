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
package org.apache.accumulo.test.metrics;

import java.io.Closeable;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.accumulo.core.util.threads.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestStatsDSink implements Closeable {

  public static class Metric {
    private final String name;
    private final String value;
    private final String type;
    private final Map<String,String> tags = new HashMap<>();

    public Metric(String name, String value, String type) {
      this.name = name;
      this.value = value;
      this.type = type;
    }

    public String getName() {
      return name;
    }

    public String getValue() {
      return value;
    }

    public String getType() {
      return type;
    }

    public Map<String,String> getTags() {
      return tags;
    }

    @Override
    public String toString() {
      return "Metric [name=" + name + ", value=" + value + ", type=" + type + ", tags=" + tags
          + "]";
    }
  }

  public static Metric parseStatsDMetric(String line) {
    int idx = line.indexOf(':');
    String name = line.substring(0, idx);
    int idx2 = line.indexOf('|');
    String value = line.substring(idx + 1, idx2);
    int idx3 = line.indexOf('|', idx2 + 1);
    String type = line.substring(idx2 + 1, idx3);
    int idx4 = line.indexOf('#');
    String tags = line.substring(idx4 + 1);
    Metric m = new Metric(name, value, type);
    String[] tag = tags.split(",");
    for (String t : tag) {
      String[] p = t.split(":");
      m.getTags().put(p[0], p[1]);
    }
    return m;
  }

  private static final Logger LOG = LoggerFactory.getLogger(TestStatsDSink.class);

  private final DatagramSocket sock;
  private final LinkedBlockingQueue<String> received = new LinkedBlockingQueue<>();

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
    List<String> metrics = new ArrayList<>(received.size());
    received.drainTo(metrics);
    return metrics;
  }

  public String getLine() {
    return received.remove();
  }

  @Override
  public void close() {
    sock.close();
  }
}
