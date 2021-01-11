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
package org.apache.accumulo.tracer;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.lang.ref.Cleaner.Cleanable;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.singletons.SingletonManager;
import org.apache.accumulo.core.singletons.SingletonReservation;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.cleaner.CleanerUtil;
import org.apache.accumulo.fate.zookeeper.ZooReader;
import org.apache.accumulo.tracer.thrift.RemoteSpan;
import org.apache.accumulo.tracer.thrift.SpanReceiver.Client;
import org.apache.htrace.HTraceConfiguration;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Find a Span collector via zookeeper and push spans there via Thrift RPC
 */
public class ZooTraceClient extends AsyncSpanReceiver<String,Client> implements Watcher {
  private static final Logger log = LoggerFactory.getLogger(ZooTraceClient.class);

  private static final int DEFAULT_TIMEOUT = 30 * 1000;

  private ZooReader zoo = null;
  private String path;
  private boolean pathExists = false;
  private final Random random = new SecureRandom();
  private final List<String> hosts = new ArrayList<>();
  private long retryPause = 5000L;
  private SingletonReservation reservation;
  private Cleanable cleanable;
  private final AtomicBoolean closed;

  ZooTraceClient() {
    closed = new AtomicBoolean(true);
  }

  public ZooTraceClient(HTraceConfiguration conf) {
    super(conf);

    String keepers = conf.get(TraceUtil.TRACER_ZK_HOST);
    if (keepers == null)
      throw new IllegalArgumentException("Must configure " + TraceUtil.TRACER_ZK_HOST);
    int timeout = conf.getInt(TraceUtil.TRACER_ZK_TIMEOUT, DEFAULT_TIMEOUT);

    // we need a client reservation for ZooSession, which is used by ZooReader
    // the reservation is registered in the Cleaner, since span receivers aren't Closeable
    closed = new AtomicBoolean(false);
    reservation = SingletonManager.getClientReservation();
    cleanable = CleanerUtil.unclosed(this, ZooTraceClient.class, closed, log, reservation);

    zoo = new ZooReader(keepers, timeout);
    path = conf.get(TraceUtil.TRACER_ZK_PATH, Constants.ZTRACERS);
    setInitialTraceHosts();
  }

  @VisibleForTesting
  protected void setRetryPause(long pause) {
    retryPause = pause;
  }

  @Override
  protected synchronized String getSpanKey(Map<String,String> data) {
    if (!hosts.isEmpty()) {
      return hosts.get(random.nextInt(hosts.size()));
    }
    return null;
  }

  @Override
  public void process(WatchedEvent event) {
    if (event.getType() == Watcher.Event.EventType.None) {
      log.debug("Ignoring event for trace server zk watch: {}", event);
      return;
    }
    log.debug("Processing event for trace server zk watch: {}", event);
    try {
      updateHostsFromZooKeeper();
    } catch (Exception ex) {
      log.error("unable to get destination hosts in zookeeper", ex);
    }
  }

  protected void setInitialTraceHosts() {
    // Make a single thread pool with a daemon thread
    final ScheduledExecutorService svc =
        Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder().setDaemon(true).build());
    final Runnable task = new Runnable() {
      @Override
      public void run() {
        try {
          updateHostsFromZooKeeper();
          log.debug("Successfully initialized tracer hosts from ZooKeeper");
          // Once this passes, we can issue a shutdown of the pool
          svc.shutdown();
        } catch (Exception e) {
          log.error("Unabled to get destination tracer hosts in ZooKeeper, will retry in "
              + retryPause + " milliseconds", e);
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
    if (hosts == null || hosts.isEmpty()) {
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

  private synchronized void updateHosts(String path, List<String> children) {
    log.debug("Scanning trace hosts in zookeeper: {}", path);
    try {
      List<String> hosts = new ArrayList<>();
      for (String child : children) {
        byte[] data = zoo.getData(path + "/" + child);
        hosts.add(new String(data, UTF_8));
      }
      this.hosts.clear();
      this.hosts.addAll(hosts);
      log.debug("Trace hosts: {}", this.hosts);
    } catch (Exception ex) {
      log.error("unable to get destination hosts in zookeeper", ex);
    }
  }

  @SuppressFBWarnings(value = "UNENCRYPTED_SOCKET",
      justification = "insecure, known risk; this is user-configurable to avoid insecure transfer")
  @Override
  protected Client createDestination(String destination) {
    if (destination == null)
      return null;
    try {
      int portSeparatorIndex = destination.lastIndexOf(':');
      String host = destination.substring(0, portSeparatorIndex);
      int port = Integer.parseInt(destination.substring(portSeparatorIndex + 1));
      log.debug("Connecting to {}:{}", host, port);
      InetSocketAddress addr = new InetSocketAddress(host, port);
      Socket sock = new Socket();
      sock.connect(addr);
      TTransport transport = new TSocket(sock);
      TProtocol prot = new TBinaryProtocol(transport);
      return new Client(prot);
    } catch (IOException ex) {
      log.trace("{}", ex, ex);
      return null;
    } catch (Exception ex) {
      log.error("{}", ex.getMessage(), ex);
      return null;
    }
  }

  @Override
  protected void send(Client client, RemoteSpan s) throws Exception {
    if (client != null) {
      try {
        client.span(s);
      } catch (Exception ex) {
        client.getInputProtocol().getTransport().close();
        throw ex;
      }
    }
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      // deregister cleanable, but it won't run because it checks
      // the value of closed first, which is now true
      cleanable.clean();
      reservation.close();
    }
    super.close();
  }
}
