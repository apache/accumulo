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
package org.apache.accumulo.server.metrics;

import java.net.URL;

import org.apache.accumulo.core.util.Daemon;
import org.apache.commons.configuration.AbstractFileConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.EnvironmentConfiguration;
import org.apache.commons.configuration.SystemConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.configuration.event.ConfigurationEvent;
import org.apache.commons.configuration.event.ConfigurationListener;
import org.apache.commons.configuration.reloading.FileChangedReloadingStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsConfiguration {

  private static final Logger log = LoggerFactory.getLogger(MetricsConfiguration.class);

  private static boolean alreadyWarned = false;

  private boolean notFound = false;

  private int notFoundCount = 0;

  private static SystemConfiguration sysConfig = null;

  private static EnvironmentConfiguration envConfig = null;

  private XMLConfiguration xConfig = null;

  private Configuration config = null;

  private final Object lock = new Object();

  private volatile boolean needsReloading = false;

  private long lastCheckTime = 0;

  private static long CONFIG_FILE_CHECK_INTERVAL = 1000 * 60 * 10; // 10 minutes

  private static int CONFIG_FILE_CHECK_COUNTER = 100;

  public final static long CONFIG_FILE_RELOAD_DELAY = 60000;

  private MetricsConfigWatcher watcher = null;

  private boolean enabled = false;

  private String enabledName = null;

  /**
   * Background thread that pokes the XMLConfiguration file to see if it has changed. If it has, then the Configuration Listener will get an event.
   *
   */
  private class MetricsConfigWatcher extends Daemon {
    public MetricsConfigWatcher() {}

    @Override
    public void run() {
      while (this.isAlive()) {
        try {
          Thread.sleep(MetricsConfiguration.CONFIG_FILE_RELOAD_DELAY);
        } catch (InterruptedException ie) {
          // Do Nothing
        }
        xConfig.getBoolean("master.enabled");
      }
    }
  }

  /**
   * ConfigurationListener that sets a flag to reload the XML config file
   */
  private class MetricsConfigListener implements ConfigurationListener {
    @Override
    public void configurationChanged(ConfigurationEvent event) {
      if (event.getType() == AbstractFileConfiguration.EVENT_RELOAD)
        needsReloading = true;
    }
  }

  public MetricsConfiguration(String name) {
    // We are going to store the "enabled" parameter for this
    // name as a shortcut so that it doesn't have to be looked
    // up in the configuration so much.
    this.enabledName = name + ".enabled";
    getMetricsConfiguration();
  }

  public Configuration getEnvironmentConfiguration() {
    synchronized (MetricsConfiguration.class) {
      if (null == envConfig)
        envConfig = new EnvironmentConfiguration();
      return envConfig;
    }
  }

  public Configuration getSystemConfiguration() {
    synchronized (MetricsConfiguration.class) {
      if (null == sysConfig)
        sysConfig = new SystemConfiguration();
      return sysConfig;
    }
  }

  public Configuration getMetricsConfiguration() {
    if (notFound) {
      if (notFoundCount <= CONFIG_FILE_CHECK_COUNTER) {
        return null;
      } else if ((notFoundCount > CONFIG_FILE_CHECK_COUNTER) && ((System.currentTimeMillis() - lastCheckTime) > CONFIG_FILE_CHECK_INTERVAL)) {
        notFoundCount = 0;
        lastCheckTime = System.currentTimeMillis();
        notFound = false;
      } else {
        notFoundCount++;
      }
    }
    if (null == config || needsReloading) {
      synchronized (lock) {
        if (needsReloading) {
          loadConfiguration();
        } else if (null == config) {
          loadConfiguration();
        }
        needsReloading = false;
      }
    }
    return config;
  }

  private void loadConfiguration() {
    URL metricsUrl = MetricsConfiguration.class.getClassLoader().getResource("accumulo-metrics.xml");
    if (metricsUrl == null) {
      if (!alreadyWarned)
        log.warn("accumulo-metrics.xml was not found on classpath. Metrics collection will be disabled.");
      alreadyWarned = true;
      notFound = true;
      return;
    }

    try {
      xConfig = new XMLConfiguration(metricsUrl);
      xConfig.append(getEnvironmentConfiguration());
      xConfig.addConfigurationListener(new MetricsConfigListener());
      xConfig.setReloadingStrategy(new FileChangedReloadingStrategy());

      // Start a background Thread that checks a property from the XMLConfiguration
      // every so often to force the FileChangedReloadingStrategy to fire.
      if (null == watcher || !watcher.isAlive()) {
        watcher = new MetricsConfigWatcher();
        watcher.start();
      }
      notFound = false;
      alreadyWarned = false;
    } catch (ConfigurationException ce) {
      log.error("Error reading accumulo-metrics.xml file.");
      notFound = true;
      return;
    }

    if (xConfig != null) {
      config = xConfig.interpolatedConfiguration();
      // set the enabled boolean from the configuration
      enabled = config.getBoolean(enabledName);
      if (log.isDebugEnabled())
        log.debug("Metrics collection enabled={}", enabled);
    } else {
      enabled = false;
    }

  }

  public boolean isEnabled() {
    // Force reload if necessary
    if (null == getMetricsConfiguration())
      return false;
    return enabled;
  }

  public static void main(String[] args) throws Exception {
    MetricsConfiguration mc = new MetricsConfiguration("master");
    while (true) {
      System.out.println("------------------------------------------------------------------------------------------------");
      long t1 = System.currentTimeMillis();
      System.out.println(mc.isEnabled() + " took: " + (System.currentTimeMillis() - t1));
      Thread.sleep(1000);
    }
  }

}
