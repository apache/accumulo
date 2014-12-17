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
package org.apache.accumulo.server.watcher;

import static com.google.common.base.Charsets.UTF_8;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.log4j.Appender;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.helpers.FileWatchdog;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import com.google.common.net.HostAndPort;

/**
 * Watcher that updates the monitor's log4j port from ZooKeeper in a system property
 */
public class MonitorLog4jWatcher extends FileWatchdog implements Watcher {
  private static final Logger log = Logger.getLogger(MonitorLog4jWatcher.class);

  private static final String HOST_PROPERTY_NAME = "org.apache.accumulo.core.host.log";
  private static final String PORT_PROPERTY_NAME = "org.apache.accumulo.core.host.log.port";

  private final Object lock;
  private final Log4jConfiguration logConfig;
  private boolean loggingDisabled = false;
  protected String path;

  public MonitorLog4jWatcher(String instance, String filename) {
    super(filename);
    this.path = ZooUtil.getRoot(instance) + Constants.ZMONITOR_LOG4J_ADDR;
    this.lock = new Object();
    this.logConfig = new Log4jConfiguration(filename);
    doOnChange();
  }

  boolean isUsingProperties() {
    return logConfig.isUsingProperties();
  }

  String getPath() {
    return path;
  }

  @Override
  public void run() {
    try {
      // Initially set the logger if the Monitor's log4j advertisement node exists
      if (ZooReaderWriter.getInstance().exists(path, this))
        updateMonitorLog4jLocation();
      log.info("Set watch for Monitor Log4j watcher");
    } catch (Exception e) {
      log.error("Unable to set watch for Monitor Log4j watcher on " + path);
    }

    super.run();
  }

  @Override
  public void doOnChange() {
    // this method gets called in the parent class' constructor
    // I'm not sure of a better way to get around this. The final modifier helps though.
    if (null == lock) {
      return;
    }

    synchronized (lock) {
      // We might triggered by file-reloading or from ZK update.
      // Either way will result in log-forwarding being restarted
      loggingDisabled = false;
      log.info("Enabled log-forwarding");
      logConfig.resetLogger();
    }
  }

  @Override
  public void process(WatchedEvent event) {
    // We got an update, process the data in the node
    updateMonitorLog4jLocation();

    if (event.getPath() != null) {
      try {
        ZooReaderWriter.getInstance().exists(event.getPath(), this);
      } catch (Exception ex) {
        log.error("Unable to reset watch for Monitor Log4j watcher", ex);
      }
    }
  }

  /**
   * Read the host and port information for the Monitor's log4j socket and update the system properties so that, on logger refresh, it sees the new information.
   */
  protected void updateMonitorLog4jLocation() {
    try {
      String hostPortString = new String(ZooReaderWriter.getInstance().getData(path, null), UTF_8);
      HostAndPort hostAndPort = HostAndPort.fromString(hostPortString);

      System.setProperty(HOST_PROPERTY_NAME, hostAndPort.getHostText());
      System.setProperty(PORT_PROPERTY_NAME, Integer.toString(hostAndPort.getPort()));

      log.info("Changing monitor log4j address to " + hostAndPort.toString());

      doOnChange();
    } catch (NoNodeException e) {
      // Not sure on the synchronization guarantees for Loggers and Appenders
      // on configuration reload
      synchronized (lock) {
        // Don't need to try to re-disable'ing it.
        if (loggingDisabled) {
          return;
        }

        Logger logger = LogManager.getLogger("org.apache.accumulo");
        if (null != logger) {
          // TODO ACCUMULO-2343 Create a specific appender for log-forwarding to the monitor
          // that can replace the AsyncAppender+SocketAppender.
          Appender appender = logger.getAppender("ASYNC");
          if (null != appender) {
            log.info("Closing log-forwarding appender");
            appender.close();
            log.info("Removing log-forwarding appender");
            logger.removeAppender(appender);
            loggingDisabled = true;
          }
        }
      }
    } catch (IllegalArgumentException e) {
      log.error("Could not parse host and port information", e);
    } catch (Exception e) {
      log.error("Error reading zookeeper data for Monitor Log4j watcher", e);
    }
  }
}
