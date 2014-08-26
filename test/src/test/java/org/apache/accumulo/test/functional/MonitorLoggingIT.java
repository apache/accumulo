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
package org.apache.accumulo.test.functional;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.net.URL;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.monitor.Monitor;
import org.apache.accumulo.server.util.Admin;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.junit.Test;

public class MonitorLoggingIT extends ConfigurableMacIT {
  public static final Logger log = Logger.getLogger(MonitorLoggingIT.class);

  @Override
  public void beforeClusterStart(MiniAccumuloConfigImpl cfg) throws Exception {
    File confDir = cfg.getConfDir();
    try {
      FileUtils.copyFileToDirectory(new File(MonitorLoggingIT.class.getResource("/conf/generic_logger.xml").toURI()), confDir);
      FileUtils.copyFileToDirectory(new File(MonitorLoggingIT.class.getResource("/conf/monitor_logger.xml").toURI()), confDir);
    } catch (Exception e) {
      log.error("Failed to copy Log4J XML files to conf dir", e);
    }
  }

  private static final int NUM_LOCATION_PASSES = 5;
  private static final int LOCATION_DELAY = 5000;

  @Override
  protected int defaultTimeoutSeconds() {
    return (NUM_LOCATION_PASSES + 2) * LOCATION_DELAY;
  }

  @Test
  public void logToMonitor() throws Exception {
    // Start the monitor.
    log.debug("Starting Monitor");
    Process monitor = cluster.exec(Monitor.class);

    // Get monitor location to ensure it is running.
    String monitorLocation = null;
    for (int i = 0; i < NUM_LOCATION_PASSES; i++) {
      Thread.sleep(LOCATION_DELAY);
      try {
        monitorLocation = getMonitor();
        break;
      } catch (KeeperException e) {
        log.debug("Monitor not up yet, trying again in " + LOCATION_DELAY + " ms");
      }
    }
    assertNotNull("Monitor failed to start within " + (LOCATION_DELAY * NUM_LOCATION_PASSES) + " ms", monitorLocation);
    log.debug("Monitor running at " + monitorLocation);

    // Attempt a scan with an invalid iterator to force a log message in the monitor.
    try {
      Connector c = getConnector();
      Scanner s = c.createScanner("accumulo.root", new Authorizations());
      IteratorSetting cfg = new IteratorSetting(100, "incorrect", "java.lang.String");
      s.addScanIterator(cfg);
      s.iterator().next();
    } catch (Exception e) {
      // expected, the iterator was bad
    }

    String result = "";
    while(true) {
      Thread.sleep(LOCATION_DELAY);  // extra precaution to ensure monitor has opportunity to log

      // Verify messages were received at the monitor.
      URL url = new URL("http://" + monitorLocation + "/log");
      log.debug("Fetching web page " + url);
      result = FunctionalTestUtils.readAll(url.openStream());
      if (result.contains("<pre class='logevent'>")) {
        break;
      }
      log.debug("No messages found, waiting a little longer...");
    }

    assertTrue("No log messages found", result.contains("<pre class='logevent'>"));

    // Shutdown cleanly.
    log.debug("Stopping mini accumulo cluster");
    Process shutdown = cluster.exec(Admin.class, "stopAll");
    shutdown.waitFor();
    assertTrue(shutdown.exitValue() == 0);
    log.debug("success!");
    monitor.destroy();
  }
}
