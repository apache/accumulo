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

import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.accumulo.fate.zookeeper.ZooCacheFactory;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.AsyncAppender;
import org.apache.log4j.net.SocketAppender;
import org.apache.zookeeper.data.Stat;

public class AccumuloMonitorAppender extends AsyncAppender implements AutoCloseable {

  final ScheduledExecutorService executorService;
  final AtomicBoolean trackerScheduled;
  private int frequency = 0;
  private MonitorTracker tracker = null;

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

  public void setFrequency(int millis) {
    if (millis > 0) {
      frequency = millis;
    }
  }

  public int getFrequency() {
    return frequency;
  }

  // this is just for testing
  void setTracker(MonitorTracker monitorTracker) {
    tracker = monitorTracker;
  }

  @Override
  public void activateOptions() {
    // only schedule it once (in case options get activated more than once); not sure if this is possible
    if (trackerScheduled.compareAndSet(false, true)) {
      if (frequency <= 0) {
        // use default rate of 5 seconds between each check
        frequency = 5000;
      }
      if (tracker == null) {
        tracker = new MonitorTracker(this, new ZooCacheLocationSupplier(), new SocketAppenderFactory());
      }
      executorService.scheduleWithFixedDelay(tracker, frequency, frequency, TimeUnit.MILLISECONDS);
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

  static class MonitorLocation {
    private final String location;
    private final long modId;

    public MonitorLocation(long modId, byte[] location) {
      this.modId = modId;
      this.location = location == null ? null : new String(location, UTF_8);
    }

    public boolean hasLocation() {
      return location != null;
    }

    public String getLocation() {
      return location;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj != null && obj instanceof MonitorLocation) {
        MonitorLocation other = (MonitorLocation) obj;
        return modId == other.modId && Objects.equals(location, other.location);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return Long.hashCode(modId);
    }
  }

  private static class ZooCacheLocationSupplier implements Supplier<MonitorLocation> {

    // path and zooCache are lazily set the first time this tracker is run
    // this allows the tracker to be constructed and scheduled during log4j initialization without
    // triggering any actual logs from the Accumulo or ZooKeeper code
    private String path = null;
    private ZooCache zooCache = null;

    @Override
    public MonitorLocation get() {
      // lazily set up path and zooCache (see comment in constructor)
      if (this.zooCache == null) {
        Instance instance = HdfsZooInstance.getInstance();
        this.path = ZooUtil.getRoot(instance) + Constants.ZMONITOR_LOG4J_ADDR;
        this.zooCache = new ZooCacheFactory().getZooCache(instance.getZooKeepers(), instance.getZooKeepersSessionTimeOut());
      }

      // get the current location from the cache and update if necessary
      Stat stat = new Stat();
      byte[] loc = zooCache.get(path, stat);
      // mzxid is 0 if location does not exist and the non-zero transaction id of the last modification otherwise
      return new MonitorLocation(stat.getMzxid(), loc);
    }
  }

  private static class SocketAppenderFactory implements Function<MonitorLocation,AppenderSkeleton> {
    @Override
    public AppenderSkeleton apply(MonitorLocation loc) {
      int defaultPort = Integer.parseUnsignedInt(Property.MONITOR_LOG4J_PORT.getDefaultValue());
      HostAndPort remote = HostAndPort.fromString(loc.getLocation());

      SocketAppender socketAppender = new SocketAppender();
      socketAppender.setApplication(System.getProperty("accumulo.application", "unknown"));
      socketAppender.setRemoteHost(remote.getHost());
      socketAppender.setPort(remote.getPortOrDefault(defaultPort));

      return socketAppender;
    }
  }

  static class MonitorTracker implements Runnable {

    private final AccumuloMonitorAppender parentAsyncAppender;
    private final Supplier<MonitorLocation> currentLocationSupplier;
    private final Function<MonitorLocation,AppenderSkeleton> appenderFactory;

    private MonitorLocation lastLocation;
    private AppenderSkeleton lastSocketAppender;

    public MonitorTracker(AccumuloMonitorAppender appender, Supplier<MonitorLocation> currentLocationSupplier,
        Function<MonitorLocation,AppenderSkeleton> appenderFactory) {
      this.parentAsyncAppender = Objects.requireNonNull(appender);
      this.appenderFactory = Objects.requireNonNull(appenderFactory);
      this.currentLocationSupplier = Objects.requireNonNull(currentLocationSupplier);

      this.lastLocation = new MonitorLocation(0, null);
      this.lastSocketAppender = null;
    }

    @Override
    public void run() {
      try {
        MonitorLocation currentLocation = currentLocationSupplier.get();
        // detect change
        if (!currentLocation.equals(lastLocation)) {
          // clean up old appender
          if (lastSocketAppender != null) {
            parentAsyncAppender.removeAppender(lastSocketAppender);
            lastSocketAppender.close();
            lastSocketAppender = null;
          }
          // create a new one
          if (currentLocation.hasLocation()) {
            lastSocketAppender = appenderFactory.apply(currentLocation);
            lastSocketAppender.activateOptions();
            parentAsyncAppender.addAppender(lastSocketAppender);
          }
          // update the last location only if switching was successful
          lastLocation = currentLocation;
        }
      } catch (Exception e) {
        // dump any non-fatal problems to the console, but let it run again
        e.printStackTrace();
      }
    }

  }

}
