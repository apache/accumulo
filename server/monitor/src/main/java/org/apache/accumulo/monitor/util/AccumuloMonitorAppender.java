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
package org.apache.accumulo.monitor.util;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.accumulo.fate.zookeeper.ZooCacheFactory;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.log4j.AsyncAppender;
import org.apache.log4j.net.SocketAppender;

import com.google.common.net.HostAndPort;

public class AccumuloMonitorAppender extends AsyncAppender {

  private final ScheduledExecutorService executorService;
  private final AtomicBoolean trackerScheduled;

  /**
   * A Log4j Appender which follows the registered location of the active Accumulo monitor service, and forwards log messages to it
   */
  public AccumuloMonitorAppender() {
    // create the background thread to watch for updates to monitor location
    trackerScheduled = new AtomicBoolean(false);
    executorService = Executors.newSingleThreadScheduledExecutor(runnable -> {
      Thread t = new Thread(runnable, "MonitorLog4jLocationTracker");
      t.setDaemon(true);
      return t;
    });
  }

  @Override
  public void activateOptions() {
    if (trackerScheduled.compareAndSet(false, true)) {
      executorService.scheduleAtFixedRate(new MonitorTracker(HdfsZooInstance.getInstance()), 5, 10, TimeUnit.SECONDS);
    }
    super.activateOptions();
  }

  @Override
  public void close() {
    if (!executorService.isShutdown()) {
      executorService.shutdownNow();
    }
    super.close();
  }

  private class MonitorTracker implements Runnable {

    private final String path;
    private final ZooCache zooCache;

    private byte[] lastLocation;
    private SocketAppender lastSocketAppender;

    public MonitorTracker(Instance instance) {
      this.path = ZooUtil.getRoot(instance) + Constants.ZMONITOR_LOG4J_ADDR;
      this.zooCache = new ZooCacheFactory().getZooCache(instance.getZooKeepers(), instance.getZooKeepersSessionTimeOut());

      this.lastLocation = null;
      this.lastSocketAppender = null;
    }

    @Override
    public void run() {
      byte[] loc = zooCache.get(path);
      if (!Arrays.equals(loc, lastLocation)) {
        // something changed
        switchAppender(loc);
      }
    }

    private void switchAppender(byte[] loc) {
      // remove and close the last one, if it was non-null
      if (lastSocketAppender != null) {
        AccumuloMonitorAppender.this.removeAppender(lastSocketAppender);
        lastSocketAppender.close();
      }

      // create a new one, if it is non-null
      if (loc != null) {

        int defaultPort = Integer.parseUnsignedInt(Property.MONITOR_LOG4J_PORT.getDefaultValue());
        HostAndPort remote = HostAndPort.fromString(new String(loc, UTF_8));

        SocketAppender socketAppender = new SocketAppender();
        socketAppender.setRemoteHost(remote.getHostText());
        socketAppender.setPort(remote.getPortOrDefault(defaultPort));

        lastLocation = loc;
        lastSocketAppender = socketAppender;

        socketAppender.activateOptions();
        AccumuloMonitorAppender.this.addAppender(socketAppender);
      }
    }

  }

}
