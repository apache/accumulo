/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.manager.upgrade;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.apache.accumulo.core.util.LazySingletons.GSON;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.core.zookeeper.ZooSession;
import org.apache.accumulo.server.AccumuloDataVersion;
import org.apache.accumulo.server.ServerContext;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

/**
 * Methods in this class are public **only** for UpgradeProgressTrackerIT
 */
public class UpgradeProgressTracker {

  /**
   * Track upgrade progress for each component. The version stored is the most recent version for
   * which an upgrade has been completed.
   */
  public static class UpgradeProgress {

    private int zooKeeperVersion;
    private int rootVersion;
    private int metadataVersion;
    private volatile transient int znodeVersion;

    public UpgradeProgress() {}

    public UpgradeProgress(int currentVersion) {
      zooKeeperVersion = currentVersion;
      rootVersion = currentVersion;
      metadataVersion = currentVersion;
    }

    public int getZooKeeperVersion() {
      return zooKeeperVersion;
    }

    public synchronized void updateZooKeeperVersion(ServerContext context, int newVersion)
        throws KeeperException, InterruptedException {
      requireNonNull(context, "ServerContext must be supplied");
      checkArgument(newVersion <= AccumuloDataVersion.get(),
          "New version (%s) cannot be larger than current data version (%s)", newVersion,
          AccumuloDataVersion.get());
      checkArgument(newVersion > zooKeeperVersion,
          "New ZooKeeper version (%s) must be greater than current version (%s)", newVersion,
          zooKeeperVersion);
      checkArgument(newVersion > rootVersion,
          "New ZooKeeper version (%s) expected to be greater than the root version (%s)",
          newVersion, rootVersion);
      checkArgument(metadataVersion == rootVersion,
          "Root (%s) and Metadata (%s) versions expected to be equal when upgrading ZooKeeper",
          rootVersion, metadataVersion);
      zooKeeperVersion = newVersion;
      put(context, this);
    }

    public int getRootVersion() {
      return rootVersion;
    }

    public synchronized void updateRootVersion(ServerContext context, int newVersion)
        throws KeeperException, InterruptedException {
      requireNonNull(context, "ServerContext must be supplied");
      checkArgument(newVersion <= AccumuloDataVersion.get(),
          "New version (%s) cannot be larger than current data version (%s)", newVersion,
          AccumuloDataVersion.get());
      checkArgument(newVersion > rootVersion,
          "New Root version (%s) must be greater than current Root version (%s)", newVersion,
          rootVersion);
      checkArgument(newVersion <= zooKeeperVersion,
          "New Root version (%s) expected to be <= ZooKeeper version (%s)", newVersion,
          zooKeeperVersion);
      checkArgument(newVersion > metadataVersion,
          "New Root version (%s) must be greater than current Metadata version (%s)", newVersion,
          metadataVersion);
      rootVersion = newVersion;
      put(context, this);
    }

    public int getMetadataVersion() {
      return metadataVersion;
    }

    public synchronized void updateMetadataVersion(ServerContext context, int newVersion)
        throws KeeperException, InterruptedException {
      requireNonNull(context, "ServerContext must be supplied");
      checkArgument(newVersion <= AccumuloDataVersion.get(),
          "New version (%s) cannot be larger than current data version (%s)", newVersion,
          AccumuloDataVersion.get());
      checkArgument(newVersion > metadataVersion,
          "New Metadata version (%s) must be greater than current version (%s)", newVersion,
          metadataVersion);
      checkArgument(newVersion <= zooKeeperVersion,
          "New Metadata version (%s) expected to be <= ZooKeeper version (%s)", newVersion,
          zooKeeperVersion);
      checkArgument(newVersion <= rootVersion,
          "New Metadata version (%s) expected to be <= Root version (%s)", newVersion, rootVersion);
      metadataVersion = newVersion;
      put(context, this);
    }
  }

  private static String getZPath(ServerContext context) {
    return context.getZooKeeperRoot() + Constants.ZUPGRADE_PROGRESS;
  }

  private static void put(ServerContext context, UpgradeProgress cv)
      throws KeeperException, InterruptedException {
    requireNonNull(context, "ServerContext must be supplied");
    requireNonNull(cv, "ComponentVersions object  must be supplied");
    final String zpath = getZPath(context);
    final ZooSession zs = context.getZooSession();
    Stat stat = zs.exists(zpath, null);
    if (stat == null) {
      zs.create(zpath, GSON.get().toJson(cv).getBytes(UTF_8), ZooUtil.PUBLIC,
          CreateMode.PERSISTENT);
      cv.znodeVersion = zs.exists(zpath, null).getVersion();
    } else {
      try {
        zs.setData(zpath, GSON.get().toJson(cv).getBytes(UTF_8), cv.znodeVersion);
      } catch (KeeperException e) {
        if (e.code() == KeeperException.Code.BADVERSION) {
          throw new IllegalStateException(
              "Upgrade progress information was updated by another process or thread.");
        }
        throw e;
      }
    }
  }

  public static UpgradeProgress get(ServerContext context)
      throws KeeperException, InterruptedException {
    final String zpath = getZPath(context);
    final int currentVersion = AccumuloDataVersion.getCurrentVersion(context);
    final ZooReaderWriter zrw = context.getZooSession().asReaderWriter();
    if (!zrw.exists(zpath)) {
      try {
        UpgradeProgress cv = new UpgradeProgress(currentVersion);
        put(context, cv);
        return cv;
      } catch (IllegalStateException ise) {
        if (ise.getMessage()
            .equals("Upgrade progress information was updated by another process or thread.")) {
          // there was a race condition, let this fall through and return the stored information
          // instead
        } else {
          throw ise;
        }
      }
    }
    Stat stat = new Stat();
    byte[] jsonData = zrw.getData(zpath, stat);
    UpgradeProgress progress =
        GSON.get().fromJson(new String(jsonData, UTF_8), UpgradeProgress.class);
    progress.znodeVersion = stat.getVersion();
    return progress;
  }

  public static synchronized void upgradeComplete(ServerContext context)
      throws KeeperException, InterruptedException {
    // This should be updated prior to deleting the tracking data in zookeeper.
    checkState(AccumuloDataVersion.getCurrentVersion(context) == AccumuloDataVersion.get(),
        "Upgrade completed, but current version (%s) is not equal to the software version (%s)",
        AccumuloDataVersion.getCurrentVersion(context), AccumuloDataVersion.get());
    final String zpath = getZPath(context);
    final ZooReaderWriter zrw = context.getZooSession().asReaderWriter();
    zrw.recursiveDelete(zpath, NodeMissingPolicy.SKIP);
  }

}
