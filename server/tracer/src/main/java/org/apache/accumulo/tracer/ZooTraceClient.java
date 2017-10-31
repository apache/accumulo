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
package org.apache.accumulo.tracer;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.trace.DistributedTrace;
import org.apache.accumulo.fate.zookeeper.ZooReader;
import org.apache.htrace.HTraceConfiguration;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Find a Span collector via zookeeper and push spans there via Thrift RPC
 */
public class ZooTraceClient extends SendSpansViaThrift implements Watcher {
  private static final Logger log = LoggerFactory.getLogger(ZooTraceClient.class);

  private static final int DEFAULT_TIMEOUT = 30 * 1000;

  ZooReader zoo = null;
  String path;
  boolean pathExists = false;
  final Random random = new Random();
  final List<String> hosts = new ArrayList<>();
  long retryPause = 5000l;

  // Visible for testing
  ZooTraceClient() {}

  public ZooTraceClient(HTraceConfiguration conf) {
    super(conf);

    String keepers = conf.get(DistributedTrace.TRACER_ZK_HOST);
    if (keepers == null)
      throw new IllegalArgumentException("Must configure " + DistributedTrace.TRACER_ZK_HOST);
    int timeout = conf.getInt(DistributedTrace.TRACER_ZK_TIMEOUT, DEFAULT_TIMEOUT);
    zoo = new ZooReader(keepers, timeout);
    path = conf.get(DistributedTrace.TRACER_ZK_PATH, Constants.ZTRACERS);
    setInitialTraceHosts();
  }

  @VisibleForTesting
  protected void setRetryPause(long pause) {
    retryPause = pause;
  }

  @Override
  synchronized protected String getSpanKey(Map<String,String> data) {
    if (hosts.size() > 0) {
      String host = hosts.get(random.nextInt(hosts.size()));
      return host;
    }
    return null;
  }

  @Override
  public void process(WatchedEvent event) {
    log.debug("Processing event for trace server zk watch");
    try {
      updateHostsFromZooKeeper();
    } catch (Exception ex) {
      log.error("unable to get destination hosts in zookeeper", ex);
    }
  }

  protected void setInitialTraceHosts() {
    // Make a single thread pool with a daemon thread
    final ScheduledExecutorService svc = Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder().setDaemon(true).build());
    final Runnable task = new Runnable() {
      @Override
      public void run() {
        try {
          updateHostsFromZooKeeper();
          log.debug("Successfully initialized tracer hosts from ZooKeeper");
          // Once this passes, we can issue a shutdown of the pool
          svc.shutdown();
        } catch (Exception e) {
          log.error("Unabled to get destination tracer hosts in ZooKeeper, will retry in " + retryPause + " milliseconds", e);
          // We failed to connect to ZK, try again in `retryPause` milliseconds
          svc.schedule(this, retryPause, TimeUnit.MILLISECONDS);
        }
      }
    };

    // Start things off
    task.run();
  }

  protected void updateHostsFromZooKeeper() throws KeeperException, InterruptedException {
    if (pathExists || zoo.exists(path)) {
      pathExists = true;
      updateHosts(path, zoo.getChildren(path, this));
    } else {
      zoo.exists(path, this);
    }
  }

  @Override
  protected void sendSpans() {
    if (hosts.isEmpty()) {
      if (!sendQueue.isEmpty()) {
        log.error("No hosts to send data to, dropping queued spans");
        synchronized (sendQueue) {
          sendQueue.clear();
          sendQueue.notifyAll();
          sendQueueSize.set(0);
        }
      }
    } else {
      super.sendSpans();
    }
  }

  synchronized private void updateHosts(String path, List<String> children) {
    log.debug("Scanning trace hosts in zookeeper: {}", path);
    try {
      List<String> hosts = new ArrayList<>();
      for (String child : children) {
        byte[] data = zoo.getData(path + "/" + child, null);
        hosts.add(new String(data, UTF_8));
      }
      this.hosts.clear();
      this.hosts.addAll(hosts);
      log.debug("Trace hosts: {}", this.hosts);
    } catch (Exception ex) {
      log.error("unable to get destination hosts in zookeeper", ex);
    }
  }
}
