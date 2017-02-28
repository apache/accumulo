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
import org.apache.zookeeper.data.Stat;

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
      Thread t = new Thread(runnable, AccumuloMonitorAppender.class.getSimpleName() + " Location Tracker");
      t.setDaemon(true);
      return t;
    });
  }

  @Override
  public void activateOptions() {
    // only schedule it once (in case options get activated more than once); not sure if this is possible
    if (trackerScheduled.compareAndSet(false, true)) {
      // wait 5 seconds, then run every 5 seconds
      executorService.scheduleAtFixedRate(new MonitorTracker(), 5, 5, TimeUnit.SECONDS);
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

    private String path;
    private ZooCache zooCache;

    private long lastModifiedTransactionId;
    private SocketAppender lastSocketAppender;

    public MonitorTracker() {

      // path and zooCache are lazily set the first time this tracker is run
      // this allows the tracker to be constructed and scheduled during log4j initialization without
      // triggering any actual logs from the Accumulo or ZooKeeper code
      this.path = null;
      this.zooCache = null;

      this.lastModifiedTransactionId = 0;
      this.lastSocketAppender = null;
    }

    @Override
    public void run() {
      try {
        // lazily set up path and zooCache (see comment in constructor)
        if (this.zooCache == null) {
          Instance instance = HdfsZooInstance.getInstance();
          this.path = ZooUtil.getRoot(instance) + Constants.ZMONITOR_LOG4J_ADDR;
          this.zooCache = new ZooCacheFactory().getZooCache(instance.getZooKeepers(), instance.getZooKeepersSessionTimeOut());
        }

        // get the current location from the cache and update if necessary
        Stat stat = new Stat();
        byte[] loc = zooCache.get(path, stat);
        long modifiedTransactionId = stat.getMzxid();

        // modifiedTransactionId will be 0 if no current location
        // lastModifiedTransactionId will be 0 if we've never seen a location
        if (modifiedTransactionId != lastModifiedTransactionId) {
          // replace old socket on every change, even if new location is the same as old location
          // if modifiedTransactionId changed, then the monitor restarted and the old socket is dead now
          switchAppender(loc, modifiedTransactionId);
        }
      } catch (Exception e) {
        // dump any non-fatal problems to the console, but let it run again
        e.printStackTrace();
      }
    }

    private void switchAppender(byte[] newLocation, long newModifiedTransactionId) {
      // remove and close the last one, if it was non-null
      if (lastSocketAppender != null) {
        AccumuloMonitorAppender.this.removeAppender(lastSocketAppender);
        lastSocketAppender.close();
      }

      // create a new SocketAppender, if new location is non-null
      if (newLocation != null) {

        int defaultPort = Integer.parseUnsignedInt(Property.MONITOR_LOG4J_PORT.getDefaultValue());
        HostAndPort remote = HostAndPort.fromString(new String(newLocation, UTF_8));

        SocketAppender socketAppender = new SocketAppender();
        socketAppender.setApplication(System.getProperty("accumulo.application", "unknown"));
        socketAppender.setRemoteHost(remote.getHostText());
        socketAppender.setPort(remote.getPortOrDefault(defaultPort));

        socketAppender.activateOptions();
        AccumuloMonitorAppender.this.addAppender(socketAppender);

        lastSocketAppender = socketAppender;
      }

      // update lastModifiedTransactionId, even if the new one is 0 (no new location)
      lastModifiedTransactionId = newModifiedTransactionId;
    }

  }

}
