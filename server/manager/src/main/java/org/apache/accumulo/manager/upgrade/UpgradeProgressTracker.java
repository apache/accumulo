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
import static java.util.Objects.requireNonNull;

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

  private final ServerContext context;

  private volatile UpgradeProgress progress;
  private volatile int znodeVersion = 0;

  public UpgradeProgressTracker(ServerContext context) {
    this.context = requireNonNull(context, "ServerContext must be supplied");
  }

  private String getZPath() {
    return context.getZooKeeperRoot() + Constants.ZUPGRADE_PROGRESS;
  }

  public synchronized void startOrContinueUpgrade() {
    var zk = context.getZooSession();
    try {
      try {
        // normally, no upgrade is in progress
        var newProgress = new UpgradeProgress(AccumuloDataVersion.getCurrentVersion(context),
            AccumuloDataVersion.get());
        zk.create(getZPath(), newProgress.toJsonBytes(), ZooUtil.PUBLIC, CreateMode.PERSISTENT);
        progress = newProgress;
        znodeVersion = 0;
      } catch (KeeperException.NodeExistsException e) {
        // existing upgrade must already be in progress
        var stat = new Stat();
        var oldProgressBytes = zk.getData(getZPath(), null, stat);
        var oldProgress = UpgradeProgress.fromJsonBytes(oldProgressBytes);
        checkState(AccumuloDataVersion.get() == oldProgress.upgradeTargetVersion,
            "Upgrade was already started with a different version of software (%s), expecting %s",
            oldProgress.upgradeTargetVersion, AccumuloDataVersion.get());
        progress = oldProgress;
        znodeVersion = stat.getVersion();
      }
    } catch (KeeperException e) {
      throw new IllegalStateException("Error initializing upgrade progress", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Error initializing upgrade progress", e);
    }
  }

  public synchronized void updateZooKeeperVersion(int newVersion) {
    checkArgument(newVersion <= AccumuloDataVersion.get(),
        "New version (%s) cannot be larger than current data version (%s)", newVersion,
        AccumuloDataVersion.get());
    checkArgument(newVersion > progress.getZooKeeperVersion(),
        "New ZooKeeper version (%s) must be greater than current version (%s)", newVersion,
        progress.getZooKeeperVersion());
    checkArgument(newVersion > progress.getRootVersion(),
        "New ZooKeeper version (%s) expected to be greater than the root version (%s)", newVersion,
        progress.getRootVersion());
    checkArgument(progress.getMetadataVersion() == progress.getRootVersion(),
        "Root (%s) and Metadata (%s) versions expected to be equal when upgrading ZooKeeper",
        progress.getRootVersion(), progress.getMetadataVersion());
    progress.zooKeeperVersion = newVersion;
    storeProgress();
  }

  public synchronized void updateRootVersion(int newVersion) {
    checkArgument(newVersion <= AccumuloDataVersion.get(),
        "New version (%s) cannot be larger than current data version (%s)", newVersion,
        AccumuloDataVersion.get());
    checkArgument(newVersion > progress.getRootVersion(),
        "New Root version (%s) must be greater than current Root version (%s)", newVersion,
        progress.getRootVersion());
    checkArgument(newVersion <= progress.getZooKeeperVersion(),
        "New Root version (%s) expected to be <= ZooKeeper version (%s)", newVersion,
        progress.getZooKeeperVersion());
    checkArgument(newVersion > progress.getMetadataVersion(),
        "New Root version (%s) must be greater than current Metadata version (%s)", newVersion,
        progress.getMetadataVersion());
    progress.rootVersion = newVersion;
    storeProgress();
  }

  public synchronized void updateMetadataVersion(int newVersion) {
    checkArgument(newVersion <= AccumuloDataVersion.get(),
        "New version (%s) cannot be larger than current data version (%s)", newVersion,
        AccumuloDataVersion.get());
    checkArgument(newVersion > progress.getMetadataVersion(),
        "New Metadata version (%s) must be greater than current version (%s)", newVersion,
        progress.getMetadataVersion());
    checkArgument(newVersion <= progress.getZooKeeperVersion(),
        "New Metadata version (%s) expected to be <= ZooKeeper version (%s)", newVersion,
        progress.getZooKeeperVersion());
    checkArgument(newVersion <= progress.getRootVersion(),
        "New Metadata version (%s) expected to be <= Root version (%s)", newVersion,
        progress.getRootVersion());
    progress.metadataVersion = newVersion;
    storeProgress();
  }

  private synchronized void storeProgress() {
    try {
      final String zpath = getZPath();
      final ZooSession zs = context.getZooSession();
      try {
        var stat = zs.setData(zpath, progress.toJsonBytes(), znodeVersion);
        znodeVersion = stat.getVersion();
      } catch (KeeperException.BadVersionException e) {
        throw new IllegalStateException(
            "Upgrade progress information was updated by another process or thread", e);
      }
    } catch (KeeperException e) {
      throw new IllegalStateException("Error storing the upgrade progress", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Error storing the upgrade progress", e);
    }
  }

  public synchronized UpgradeProgress getProgress() {
    return requireNonNull(progress,
        "Must call startOrContinueUpgrade() before checking the progress");
  }

  public synchronized void upgradeComplete() {
    // This should be updated prior to deleting the tracking data in zookeeper.
    checkState(AccumuloDataVersion.getCurrentVersion(context) == AccumuloDataVersion.get(),
        "Upgrade completed, but current version (%s) is not equal to the software version (%s)",
        AccumuloDataVersion.getCurrentVersion(context), AccumuloDataVersion.get());
    final ZooReaderWriter zrw = context.getZooSession().asReaderWriter();
    try {
      zrw.recursiveDelete(getZPath(), NodeMissingPolicy.SKIP);
    } catch (KeeperException e) {
      throw new IllegalStateException("Error clearing the upgrade progress", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Error clearing the upgrade progress", e);
    }
  }

}
