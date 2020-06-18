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
package org.apache.accumulo.server;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.util.AddressUtil;
import org.apache.accumulo.core.volume.Volume;
import org.apache.accumulo.fate.ReadOnlyStore;
import org.apache.accumulo.fate.ReadOnlyTStore;
import org.apache.accumulo.fate.ZooStore;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.util.time.SimpleTimer;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerUtil {

  private static final Logger log = LoggerFactory.getLogger(ServerUtil.class);

  public static synchronized void updateAccumuloVersion(VolumeManager fs, int oldVersion) {
    for (Volume volume : fs.getVolumes()) {
      try {
        if (getAccumuloPersistentVersion(volume) == oldVersion) {
          log.debug("Attempting to upgrade {}", volume);
          Path dataVersionLocation = ServerConstants.getDataVersionLocation(volume);
          fs.create(new Path(dataVersionLocation, Integer.toString(ServerConstants.DATA_VERSION)))
              .close();
          // TODO document failure mode & recovery if FS permissions cause above to work and below
          // to fail ACCUMULO-2596
          Path prevDataVersionLoc = new Path(dataVersionLocation, Integer.toString(oldVersion));
          if (!fs.delete(prevDataVersionLoc)) {
            throw new RuntimeException("Could not delete previous data version location ("
                + prevDataVersionLoc + ") for " + volume);
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

  public static synchronized int getAccumuloPersistentVersion(Volume v) {
    Path path = ServerConstants.getDataVersionLocation(v);
    return getAccumuloPersistentVersion(v.getFileSystem(), path);
  }

  public static synchronized int getAccumuloPersistentVersion(VolumeManager fs) {
    // It doesn't matter which Volume is used as they should all have the data version stored
    return getAccumuloPersistentVersion(fs.getVolumes().iterator().next());
  }

  public static synchronized Path getAccumuloInstanceIdPath(VolumeManager fs) {
    // It doesn't matter which Volume is used as they should all have the instance ID stored
    Volume v = fs.getVolumes().iterator().next();
    return ServerConstants.getInstanceIdLocation(v);
  }

  public static void init(ServerContext context, String application) {
    final AccumuloConfiguration conf = context.getConfiguration();

    log.info("{} starting", application);
    log.info("Instance {}", context.getInstanceID());
    int dataVersion = ServerUtil.getAccumuloPersistentVersion(context.getVolumeManager());
    log.info("Data Version {}", dataVersion);
    ServerUtil.waitForZookeeperAndHdfs(context);

    ensureDataVersionCompatible(dataVersion);

    TreeMap<String,String> sortedProps = new TreeMap<>();
    for (Entry<String,String> entry : conf)
      sortedProps.put(entry.getKey(), entry.getValue());

    for (Entry<String,String> entry : sortedProps.entrySet()) {
      String key = entry.getKey();
      log.info("{} = {}", key, (Property.isSensitive(key) ? "<hidden>" : entry.getValue()));
      Property prop = Property.getPropertyByKey(key);
      if (prop != null && conf.isPropertySet(prop, false)) {
        if (prop.isDeprecated()) {
          Property replacedBy = prop.replacedBy();
          if (replacedBy != null) {
            log.warn("{} is deprecated, use {} instead.", prop.getKey(), replacedBy.getKey());
          } else {
            log.warn("{} is deprecated", prop.getKey());
          }
        }
      }
    }

    monitorSwappiness(conf);

    // Encourage users to configure TLS
    final String SSL = "SSL";
    for (Property sslProtocolProperty : Arrays.asList(Property.RPC_SSL_CLIENT_PROTOCOL,
        Property.RPC_SSL_ENABLED_PROTOCOLS, Property.MONITOR_SSL_INCLUDE_PROTOCOLS)) {
      String value = conf.get(sslProtocolProperty);
      if (value.contains(SSL)) {
        log.warn("It is recommended that {} only allow TLS", sslProtocolProperty);
      }
    }
  }

  /**
   * Check to see if this version of Accumulo can run against or upgrade the passed in data version.
   */
  public static void ensureDataVersionCompatible(int dataVersion) {
    if (!(ServerConstants.CAN_RUN.contains(dataVersion))) {
      throw new IllegalStateException("This version of accumulo (" + Constants.VERSION
          + ") is not compatible with files stored using data version " + dataVersion);
    }
  }

  /**
   * Does the data version number stored in the backing Volumes indicate we need to upgrade
   * something?
   */
  public static boolean persistentVersionNeedsUpgrade(final int accumuloPersistentVersion) {
    return ServerConstants.NEEDS_UPGRADE.contains(accumuloPersistentVersion);
  }

  public static void monitorSwappiness(AccumuloConfiguration config) {
    SimpleTimer.getInstance(config).schedule(() -> {
      try {
        String procFile = "/proc/sys/vm/swappiness";
        File swappiness = new File(procFile);
        if (swappiness.exists() && swappiness.canRead()) {
          try (InputStream is = new FileInputStream(procFile)) {
            byte[] buffer = new byte[10];
            int bytes = is.read(buffer);
            String setting = new String(buffer, 0, bytes, UTF_8);
            setting = setting.trim();
            if (bytes > 0 && Integer.parseInt(setting) > 10) {
              log.warn("System swappiness setting is greater than ten ({})"
                  + " which can cause time-sensitive operations to be delayed."
                  + " Accumulo is time sensitive because it needs to maintain"
                  + " distributed lock agreement.", setting);
            }
          }
        }
      } catch (Throwable t) {
        log.error("", t);
      }
    }, 1000, 10 * 60 * 1000);
  }

  public static void waitForZookeeperAndHdfs(ServerContext context) {
    log.info("Attempting to talk to zookeeper");
    while (true) {
      try {
        context.getZooReaderWriter().getChildren(Constants.ZROOT);
        break;
      } catch (InterruptedException e) {
        // ignored
      } catch (KeeperException ex) {
        log.info("Waiting for accumulo to be initialized");
        sleepUninterruptibly(1, TimeUnit.SECONDS);
      }
    }
    log.info("ZooKeeper connected and initialized, attempting to talk to HDFS");
    long sleep = 1000;
    int unknownHostTries = 3;
    while (true) {
      try {
        if (context.getVolumeManager().isReady())
          break;
        log.warn("Waiting for the NameNode to leave safemode");
      } catch (IOException ex) {
        log.warn("Unable to connect to HDFS", ex);
      } catch (IllegalArgumentException e) {
        /* Unwrap the UnknownHostException so we can deal with it directly */
        if (e.getCause() instanceof UnknownHostException) {
          if (unknownHostTries > 0) {
            log.warn("Unable to connect to HDFS, will retry. cause: {}", e.getCause());
            /*
             * We need to make sure our sleep period is long enough to avoid getting a cached
             * failure of the host lookup.
             */
            int ttl = AddressUtil.getAddressCacheNegativeTtl((UnknownHostException) e.getCause());
            sleep = Math.max(sleep, (ttl + 1) * 1000L);
          } else {
            log.error("Unable to connect to HDFS and exceeded the maximum number of retries.", e);
            throw e;
          }
          unknownHostTries--;
        } else {
          throw e;
        }
      }
      log.info("Backing off due to failure; current sleep period is {} seconds", sleep / 1000.);
      sleepUninterruptibly(sleep, TimeUnit.MILLISECONDS);
      /* Back off to give transient failures more time to clear. */
      sleep = Math.min(60 * 1000, sleep * 2);
    }
    log.info("Connected to HDFS");
  }

  /**
   * Exit loudly if there are outstanding Fate operations. Since Fate serializes class names, we
   * need to make sure there are no queued transactions from a previous version before continuing an
   * upgrade. The status of the operations is irrelevant; those in SUCCESSFUL status cause the same
   * problem as those just queued.
   *
   * Note that the Master should not allow write access to Fate until after all upgrade steps are
   * complete.
   *
   * Should be called as a guard before performing any upgrade steps, after determining that an
   * upgrade is needed.
   *
   * see ACCUMULO-2519
   */
  public static void abortIfFateTransactions(ServerContext context) {
    try {
      final ReadOnlyTStore<ServerUtil> fate =
          new ReadOnlyStore<>(new ZooStore<>(context.getZooKeeperRoot() + Constants.ZFATE,
              context.getZooReaderWriter()));
      if (!(fate.list().isEmpty())) {
        throw new AccumuloException("Aborting upgrade because there are"
            + " outstanding FATE transactions from a previous Accumulo version."
            + " You can start the tservers and then use the shell to delete completed "
            + " transactions. If there are uncomplete transactions, you will need to roll"
            + " back and fix those issues. Please see the following page for more information: "
            + " https://accumulo.apache.org/docs/2.x/troubleshooting/advanced#upgrade-issues");
      }
    } catch (Exception exception) {
      log.error("Problem verifying Fate readiness", exception);
      System.exit(1);
    }
  }
}
