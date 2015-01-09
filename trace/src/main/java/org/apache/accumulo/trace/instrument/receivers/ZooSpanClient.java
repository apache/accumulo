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
package org.apache.accumulo.trace.instrument.receivers;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;

/**
 * Find a Span collector via zookeeper and push spans there via Thrift RPC
 *
 */
public class ZooSpanClient extends SendSpansViaThrift {

  private static final Logger log = Logger.getLogger(ZooSpanClient.class);
  private static final int TOTAL_TIME_WAIT_CONNECT_MS = 10 * 1000;
  private static final int TIME_WAIT_CONNECT_CHECK_MS = 100;
  private static final Charset UTF_8 = Charset.forName("UTF-8");

  ZooKeeper zoo = null;
  final String path;
  final Random random = new Random();
  final List<String> hosts = new ArrayList<String>();

  public ZooSpanClient(String keepers, final String path, String host, String service, long millis) throws IOException, KeeperException, InterruptedException {
    super(host, service, millis);
    this.path = path;
    zoo = new ZooKeeper(keepers, 30 * 1000, new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        try {
          if (zoo != null) {
            updateHosts(path, zoo.getChildren(path, null));
          }
        } catch (Exception ex) {
          log.error("unable to get destination hosts in zookeeper", ex);
        }
      }
    });
    for (int i = 0; i < TOTAL_TIME_WAIT_CONNECT_MS; i += TIME_WAIT_CONNECT_CHECK_MS) {
      if (zoo.getState().equals(States.CONNECTED))
        break;
      try {
        Thread.sleep(TIME_WAIT_CONNECT_CHECK_MS);
      } catch (InterruptedException ex) {
        break;
      }
    }
    zoo.getChildren(path, true);
  }

  @Override
  public void flush() {
    if (!hosts.isEmpty())
      super.flush();
  }

  @Override
  void sendSpans() {
    if (hosts.isEmpty()) {
      if (!sendQueue.isEmpty()) {
        log.error("No hosts to send data to, dropping queued spans");
        synchronized (sendQueue) {
          sendQueue.clear();
          sendQueue.notifyAll();
        }
      }
    } else {
      super.sendSpans();
    }
  }

  synchronized private void updateHosts(String path, List<String> children) {
    log.debug("Scanning trace hosts in zookeeper: " + path);
    try {
      List<String> hosts = new ArrayList<String>();
      for (String child : children) {
        byte[] data = zoo.getData(path + "/" + child, null, null);
        hosts.add(new String(data, UTF_8));
      }
      this.hosts.clear();
      this.hosts.addAll(hosts);
      log.debug("Trace hosts: " + this.hosts);
    } catch (Exception ex) {
      log.error("unable to get destination hosts in zookeeper", ex);
    }
  }

  @Override
  synchronized protected String getSpanKey(Map<String,String> data) {
    if (hosts.size() > 0) {
      String host = hosts.get(random.nextInt(hosts.size()));
      log.debug("sending data to " + host);
      return host;
    }
    return null;
  }
}
