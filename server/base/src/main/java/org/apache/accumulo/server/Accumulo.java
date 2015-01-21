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
package org.apache.accumulo.server;

import static com.google.common.base.Charsets.UTF_8;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.trace.DistributedTrace;
import org.apache.accumulo.core.util.AddressUtil;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.core.util.Version;
import org.apache.accumulo.core.volume.Volume;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.ReadOnlyStore;
import org.apache.accumulo.fate.ReadOnlyTStore;
import org.apache.accumulo.fate.ZooStore;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.util.time.SimpleTimer;
import org.apache.accumulo.server.watcher.Log4jConfiguration;
import org.apache.accumulo.server.watcher.MonitorLog4jWatcher;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.log4j.helpers.LogLog;
import org.apache.zookeeper.KeeperException;

public class Accumulo {

  private static final Logger log = Logger.getLogger(Accumulo.class);

  public static synchronized void updateAccumuloVersion(VolumeManager fs, int oldVersion) {
    for (Volume volume : fs.getVolumes()) {
      try {
        if (getAccumuloPersistentVersion(fs) == oldVersion) {
          log.debug("Attempting to upgrade " + volume);
          Path dataVersionLocation = ServerConstants.getDataVersionLocation(volume);
          fs.create(new Path(dataVersionLocation, Integer.toString(ServerConstants.DATA_VERSION))).close();
          // TODO document failure mode & recovery if FS permissions cause above to work and below to fail ACCUMULO-2596
          Path prevDataVersionLoc = new Path(dataVersionLocation, Integer.toString(oldVersion));
          if (!fs.delete(prevDataVersionLoc)) {
            throw new RuntimeException("Could not delete previous data version location (" + prevDataVersionLoc + ") for " + volume);
          }
        }
      } catch (IOException e) {
        throw new RuntimeException("Unable to set accumulo version: an error occurred.", e);
      }
    }
  }

  public static synchronized int getAccumuloPersistentVersion(FileSystem fs, Path path) {
    int dataVersion;
    try {
      FileStatus[] files = fs.listStatus(path);
      if (files == null || files.length == 0) {
        dataVersion = -1; // assume it is 0.5 or earlier
      } else {
        dataVersion = Integer.parseInt(files[0].getPath().getName());
      }
      return dataVersion;
    } catch (IOException e) {
      throw new RuntimeException("Unable to read accumulo version: an error occurred.", e);
    }
  }

  public static synchronized int getAccumuloPersistentVersion(VolumeManager fs) {
    // It doesn't matter which Volume is used as they should all have the data version stored
    Volume v = fs.getVolumes().iterator().next();
    Path path = ServerConstants.getDataVersionLocation(v);
    return getAccumuloPersistentVersion(v.getFileSystem(), path);
  }

  public static synchronized Path getAccumuloInstanceIdPath(VolumeManager fs) {
    // It doesn't matter which Volume is used as they should all have the instance ID stored
    Volume v = fs.getVolumes().iterator().next();
    return ServerConstants.getInstanceIdLocation(v);
  }

  public static void enableTracing(String address, String application) {
    try {
      DistributedTrace.enable(HdfsZooInstance.getInstance(), ZooReaderWriter.getInstance(), application, address);
    } catch (Exception ex) {
      log.error("creating remote sink for trace spans", ex);
    }
  }

  /**
   * Finds the best log4j configuration file. A generic file is used only if an application-specific file is not available. An XML file is preferred over a
   * properties file, if possible.
   *
   * @param confDir
   *          directory where configuration files should reside
   * @param application
   *          application name for configuration file name
   * @return configuration file name
   */
  static String locateLogConfig(String confDir, String application) {
    String explicitConfigFile = System.getProperty("log4j.configuration");
    if (explicitConfigFile != null) {
      return explicitConfigFile;
    }
    String[] configFiles = {String.format("%s/%s_logger.xml", confDir, application), String.format("%s/%s_logger.properties", confDir, application),
        String.format("%s/generic_logger.xml", confDir), String.format("%s/generic_logger.properties", confDir)};
    String defaultConfigFile = configFiles[2]; // generic_logger.xml
    for (String f : configFiles) {
      if (new File(f).exists()) {
        return f;
      }
    }
    return defaultConfigFile;
  }

  public static void setupLogging(String application) throws UnknownHostException {
    System.setProperty("org.apache.accumulo.core.application", application);

    if (System.getenv("ACCUMULO_LOG_DIR") != null)
      System.setProperty("org.apache.accumulo.core.dir.log", System.getenv("ACCUMULO_LOG_DIR"));
    else
      System.setProperty("org.apache.accumulo.core.dir.log", System.getenv("ACCUMULO_HOME") + "/logs/");

    String localhost = InetAddress.getLocalHost().getHostName();
    System.setProperty("org.apache.accumulo.core.ip.localhost.hostname", localhost);

    // Use a specific log config, if it exists
    String logConfigFile = locateLogConfig(System.getenv("ACCUMULO_CONF_DIR"), application);
    // Turn off messages about not being able to reach the remote logger... we protect against that.
    LogLog.setQuietMode(true);

    // Set up local file-based logging right away
    Log4jConfiguration logConf = new Log4jConfiguration(logConfigFile);
    logConf.resetLogger();
  }

  public static void init(VolumeManager fs, ServerConfiguration serverConfig, String application) throws IOException {
    final AccumuloConfiguration conf = serverConfig.getConfiguration();
    final Instance instance = serverConfig.getInstance();

    // Use a specific log config, if it exists
    final String logConfigFile = locateLogConfig(System.getenv("ACCUMULO_CONF_DIR"), application);

    // Set up polling log4j updates and log-forwarding using information advertised in zookeeper by the monitor
    MonitorLog4jWatcher logConfigWatcher = new MonitorLog4jWatcher(instance.getInstanceID(), logConfigFile);
    logConfigWatcher.setDelay(5000L);
    logConfigWatcher.start();

    // Makes sure the log-forwarding to the monitor is configured
    int logPort = conf.getPort(Property.MONITOR_LOG4J_PORT);
    System.setProperty("org.apache.accumulo.core.host.log.port", Integer.toString(logPort));

    log.info(application + " starting");
    log.info("Instance " + serverConfig.getInstance().getInstanceID());
    int dataVersion = Accumulo.getAccumuloPersistentVersion(fs);
    log.info("Data Version " + dataVersion);
    Accumulo.waitForZookeeperAndHdfs(fs);

    Version codeVersion = new Version(Constants.VERSION);
    if (!(canUpgradeFromDataVersion(dataVersion))) {
      throw new RuntimeException("This version of accumulo (" + codeVersion + ") is not compatible with files stored using data version " + dataVersion);
    }

    TreeMap<String,String> sortedProps = new TreeMap<String,String>();
    for (Entry<String,String> entry : conf)
      sortedProps.put(entry.getKey(), entry.getValue());

    for (Entry<String,String> entry : sortedProps.entrySet()) {
      String key = entry.getKey();
      log.info(key + " = " + (Property.isSensitive(key) ? "<hidden>" : entry.getValue()));
    }

    monitorSwappiness();

    // Encourage users to configure TLS
    final String SSL = "SSL";
    for (Property sslProtocolProperty : Arrays.asList(Property.RPC_SSL_CLIENT_PROTOCOL, Property.RPC_SSL_ENABLED_PROTOCOLS,
        Property.MONITOR_SSL_INCLUDE_PROTOCOLS)) {
      String value = conf.get(sslProtocolProperty);
      if (value.contains(SSL)) {
        log.warn("It is recommended that " + sslProtocolProperty + " only allow TLS");
      }
    }

  }

  /**
   * Sanity check that the current persistent version is allowed to upgrade to the version of Accumulo running.
   *
   * @param dataVersion
   *          the version that is persisted in the backing Volumes
   */
  public static boolean canUpgradeFromDataVersion(final int dataVersion) {
    return dataVersion == ServerConstants.DATA_VERSION || dataVersion == ServerConstants.PREV_DATA_VERSION
        || dataVersion == ServerConstants.TWO_DATA_VERSIONS_AGO;
  }

  /**
   * Does the data version number stored in the backing Volumes indicate we need to upgrade something?
   */
  public static boolean persistentVersionNeedsUpgrade(final int accumuloPersistentVersion) {
    return accumuloPersistentVersion == ServerConstants.TWO_DATA_VERSIONS_AGO || accumuloPersistentVersion == ServerConstants.PREV_DATA_VERSION;
  }

  /**
   *
   */
  public static void monitorSwappiness() {
    SimpleTimer.getInstance().schedule(new Runnable() {
      @Override
      public void run() {
        try {
          String procFile = "/proc/sys/vm/swappiness";
          File swappiness = new File(procFile);
          if (swappiness.exists() && swappiness.canRead()) {
            InputStream is = new FileInputStream(procFile);
            try {
              byte[] buffer = new byte[10];
              int bytes = is.read(buffer);
              String setting = new String(buffer, 0, bytes, UTF_8);
              setting = setting.trim();
              if (bytes > 0 && Integer.parseInt(setting) > 10) {
                log.warn("System swappiness setting is greater than ten (" + setting + ") which can cause time-sensitive operations to be delayed. "
                    + " Accumulo is time sensitive because it needs to maintain distributed lock agreement.");
              }
            } finally {
              is.close();
            }
          }
        } catch (Throwable t) {
          log.error(t, t);
        }
      }
    }, 1000, 10 * 60 * 1000);
  }

  public static void waitForZookeeperAndHdfs(VolumeManager fs) {
    log.info("Attempting to talk to zookeeper");
    while (true) {
      try {
        ZooReaderWriter.getInstance().getChildren(Constants.ZROOT);
        break;
      } catch (InterruptedException e) {
        // ignored
      } catch (KeeperException ex) {
        log.info("Waiting for accumulo to be initialized");
        UtilWaitThread.sleep(1000);
      }
    }
    log.info("ZooKeeper connected and initialized, attempting to talk to HDFS");
    long sleep = 1000;
    int unknownHostTries = 3;
    while (true) {
      try {
        if (fs.isReady())
          break;
        log.warn("Waiting for the NameNode to leave safemode");
      } catch (IOException ex) {
        log.warn("Unable to connect to HDFS", ex);
      } catch (IllegalArgumentException exception) {
        /* Unwrap the UnknownHostException so we can deal with it directly */
        if (exception.getCause() instanceof UnknownHostException) {
          if (unknownHostTries > 0) {
            log.warn("Unable to connect to HDFS, will retry. cause: " + exception.getCause());
            /* We need to make sure our sleep period is long enough to avoid getting a cached failure of the host lookup. */
            sleep = Math.max(sleep, (AddressUtil.getAddressCacheNegativeTtl((UnknownHostException) (exception.getCause())) + 1) * 1000);
          } else {
            log.error("Unable to connect to HDFS and have exceeded the maximum number of retries.", exception);
            throw exception;
          }
          unknownHostTries--;
        } else {
          throw exception;
        }
      }
      log.info("Backing off due to failure; current sleep period is " + sleep / 1000. + " seconds");
      UtilWaitThread.sleep(sleep);
      /* Back off to give transient failures more time to clear. */
      sleep = Math.min(60 * 1000, sleep * 2);
    }
    log.info("Connected to HDFS");
  }

  /**
   * Exit loudly if there are outstanding Fate operations. Since Fate serializes class names, we need to make sure there are no queued transactions from a
   * previous version before continuing an upgrade. The status of the operations is irrelevant; those in SUCCESSFUL status cause the same problem as those just
   * queued.
   *
   * Note that the Master should not allow write access to Fate until after all upgrade steps are complete.
   *
   * Should be called as a guard before performing any upgrade steps, after determining that an upgrade is needed.
   *
   * see ACCUMULO-2519
   */
  public static void abortIfFateTransactions() {
    try {
      final ReadOnlyTStore<Accumulo> fate = new ReadOnlyStore<Accumulo>(new ZooStore<Accumulo>(
          ZooUtil.getRoot(HdfsZooInstance.getInstance()) + Constants.ZFATE, ZooReaderWriter.getInstance()));
      if (!(fate.list().isEmpty())) {
        throw new AccumuloException("Aborting upgrade because there are outstanding FATE transactions from a previous Accumulo version. "
            + "Please see the README document for instructions on what to do under your previous version.");
      }
    } catch (Exception exception) {
      log.fatal("Problem verifying Fate readiness", exception);
      System.exit(1);
    }
  }
}
