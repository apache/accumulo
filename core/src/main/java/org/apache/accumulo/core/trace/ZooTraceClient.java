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
package org.apache.accumulo.core.trace;

import static com.google.common.base.Charsets.UTF_8;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.accumulo.fate.zookeeper.ZooReader;
import org.apache.accumulo.trace.instrument.receivers.SendSpansViaThrift;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

/**
 * Find a Span collector via zookeeper and push spans there via Thrift RPC
 *
 */
public class ZooTraceClient extends SendSpansViaThrift implements Watcher {

  private static final Logger log = Logger.getLogger(ZooTraceClient.class);

  final ZooReader zoo;
  final String path;
  final Random random = new Random();
  final List<String> hosts = new ArrayList<String>();

  public ZooTraceClient(ZooReader zoo, String path, String host, String service, long millis) throws IOException, KeeperException, InterruptedException {
    super(host, service, millis);
    this.path = path;
    this.zoo = zoo;
    updateHosts(path, zoo.getChildren(path, this));
  }

  @Override
  synchronized protected String getSpanKey(Map<String,String> data) {
    if (hosts.size() > 0) {
      return hosts.get(random.nextInt(hosts.size()));
    }
    return null;
  }

  @Override
  public void process(WatchedEvent event) {
    try {
      updateHosts(path, zoo.getChildren(path, null));
    } catch (Exception ex) {
      log.error("unable to get destination hosts in zookeeper", ex);
    }
  }

  synchronized private void updateHosts(String path, List<String> children) {
    log.debug("Scanning trace hosts in zookeeper: " + path);
    try {
      List<String> hosts = new ArrayList<String>();
      for (String child : children) {
        byte[] data = zoo.getData(path + "/" + child, null);
        hosts.add(new String(data, UTF_8));
      }
      this.hosts.clear();
      this.hosts.addAll(hosts);
      log.debug("Trace hosts: " + this.hosts);
    } catch (Exception ex) {
      log.error("unable to get destination hosts in zookeeper", ex);
    }
  }
}
