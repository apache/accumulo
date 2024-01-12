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

import static org.apache.accumulo.server.AccumuloDataVersion.METADATA_FILE_JSON_ENCODING;
import static org.apache.accumulo.server.AccumuloDataVersion.REMOVE_DEPRECATIONS_FOR_VERSION_3;
import static org.apache.accumulo.server.AccumuloDataVersion.ROOT_TABLET_META_CHANGES;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.fate.ReadOnlyFateStore;
import org.apache.accumulo.core.fate.ZooStore;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.core.volume.Volume;
import org.apache.accumulo.manager.EventCoordinator;
import org.apache.accumulo.server.AccumuloDataVersion;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.ServerDirs;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class UpgradeCoordinator {

  public enum UpgradeStatus {
    /**
     * This signifies the upgrade status is in the process of being determined. It is best to assume
     * nothing is upgraded when seeing this.
     */
    INITIAL {
      @Override
      public boolean isParentLevelUpgraded(Ample.DataLevel level) {
        return false;
      }
    },
    /**
     * This signifies that only zookeeper has been upgraded so far.
     */
    UPGRADED_ZOOKEEPER {
      @Override
      public boolean isParentLevelUpgraded(Ample.DataLevel level) {
        return level == Ample.DataLevel.ROOT;
      }
    },
    /**
     * This signifies that only zookeeper and the root table have been upgraded so far.
     */
    UPGRADED_ROOT {
      @Override
      public boolean isParentLevelUpgraded(Ample.DataLevel level) {
        return level == Ample.DataLevel.METADATA || level == Ample.DataLevel.ROOT;
      }
    },
    /**
     * This signifies that everything (zookeeper, root table, metadata table) is upgraded.
     */
    COMPLETE {
      @Override
      public boolean isParentLevelUpgraded(Ample.DataLevel level) {
        return true;
      }
    },
    /**
     * This signifies a failure occurred during upgrade.
     */
    FAILED {
      @Override
      public boolean isParentLevelUpgraded(Ample.DataLevel level) {
        return false;
      }
    };

    /**
     * Determines if the place where this extent stores its metadata was upgraded for a given
     * upgrade status.
     */
    public abstract boolean isParentLevelUpgraded(Ample.DataLevel level);
  }

  private static final Logger log = LoggerFactory.getLogger(UpgradeCoordinator.class);

  private int currentVersion;
  // map of "current version" -> upgrader to next version.
  // Sorted so upgrades execute in order from the oldest supported data version to current
  private final Map<Integer,
      Upgrader> upgraders = Collections.unmodifiableMap(new TreeMap<>(
          Map.of(ROOT_TABLET_META_CHANGES, new Upgrader10to11(), REMOVE_DEPRECATIONS_FOR_VERSION_3,
              new Upgrader11to12(), METADATA_FILE_JSON_ENCODING, new Upgrader12to13())));

  private volatile UpgradeStatus status;

  public UpgradeCoordinator() {
    status = UpgradeStatus.INITIAL;
  }

  private void setStatus(UpgradeStatus status, EventCoordinator eventCoordinator) {
    UpgradeStatus oldStatus = this.status;
    this.status = status;
    // calling this will wake up threads that may assign tablets. After the upgrade status changes
    // those threads may make different assignment decisions.
    eventCoordinator.event("Upgrade status changed from %s to %s", oldStatus, status);
  }

  @SuppressFBWarnings(value = "DM_EXIT",
      justification = "Want to immediately stop all manager threads on upgrade error")
  private void handleFailure(Exception e) {
    log.error("FATAL: Error performing upgrade", e);
    // do not want to call setStatus and signal an event in this case
    status = UpgradeStatus.FAILED;
    System.exit(1);
  }

  public synchronized void upgradeZookeeper(ServerContext context,
      EventCoordinator eventCoordinator) {

    Preconditions.checkState(status == UpgradeStatus.INITIAL,
        "Not currently in a suitable state to do zookeeper upgrade %s", status);

    try {
      int cv = AccumuloDataVersion.getCurrentVersion(context);
      this.currentVersion = cv;

      if (cv == AccumuloDataVersion.get()) {
        status = UpgradeStatus.COMPLETE;
        return;
      }

      if (currentVersion < AccumuloDataVersion.get()) {
        abortIfFateTransactions(context);

        for (int v = currentVersion; v < AccumuloDataVersion.get(); v++) {
          log.info("Upgrading Zookeeper - current version {} as step towards target version {}", v,
              AccumuloDataVersion.get());
          var upgrader = upgraders.get(v);
          Objects.requireNonNull(upgrader,
              "upgrade ZooKeeper: failed to find upgrader for version " + currentVersion);
          upgrader.upgradeZookeeper(context);
        }
      }

      setStatus(UpgradeStatus.UPGRADED_ZOOKEEPER, eventCoordinator);
    } catch (Exception e) {
      handleFailure(e);
    }

  }

  public synchronized Future<Void> upgradeMetadata(ServerContext context,
      EventCoordinator eventCoordinator) {
    if (status == UpgradeStatus.COMPLETE) {
      return CompletableFuture.completedFuture(null);
    }

    Preconditions.checkState(status == UpgradeStatus.UPGRADED_ZOOKEEPER,
        "Not currently in a suitable state to do metadata upgrade %s", status);

    if (currentVersion < AccumuloDataVersion.get()) {
      return ThreadPools.getServerThreadPools().createThreadPool(0, Integer.MAX_VALUE, 60L,
          TimeUnit.SECONDS, "UpgradeMetadataThreads", new SynchronousQueue<>(), false)
          .submit(() -> {
            try {
              for (int v = currentVersion; v < AccumuloDataVersion.get(); v++) {
                log.info("Upgrading Root - current version {} as step towards target version {}", v,
                    AccumuloDataVersion.get());
                var upgrader = upgraders.get(v);
                Objects.requireNonNull(upgrader,
                    "upgrade root: failed to find root upgrader for version " + currentVersion);
                upgraders.get(v).upgradeRoot(context);
              }

              setStatus(UpgradeStatus.UPGRADED_ROOT, eventCoordinator);

              for (int v = currentVersion; v < AccumuloDataVersion.get(); v++) {
                log.info(
                    "Upgrading Metadata - current version {} as step towards target version {}", v,
                    AccumuloDataVersion.get());
                var upgrader = upgraders.get(v);
                Objects.requireNonNull(upgrader,
                    "upgrade metadata: failed to find upgrader for version " + currentVersion);
                upgraders.get(v).upgradeMetadata(context);
              }

              log.info("Updating persistent data version.");
              updateAccumuloVersion(context.getServerDirs(), context.getVolumeManager(),
                  currentVersion);
              log.info("Upgrade complete");
              setStatus(UpgradeStatus.COMPLETE, eventCoordinator);
            } catch (Exception e) {
              handleFailure(e);
            }
            return null;
          });
    } else {
      return CompletableFuture.completedFuture(null);
    }
  }

  // visible for testing
  synchronized void updateAccumuloVersion(ServerDirs serverDirs, VolumeManager fs, int oldVersion) {
    for (Volume volume : fs.getVolumes()) {
      try {
        if (serverDirs.getAccumuloPersistentVersion(volume) == oldVersion) {
          log.debug("Attempting to upgrade {}", volume);
          Path dataVersionLocation = serverDirs.getDataVersionLocation(volume);
          fs.create(new Path(dataVersionLocation, Integer.toString(AccumuloDataVersion.get())))
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

  public UpgradeStatus getStatus() {
    return status;
  }

  /**
   * Exit loudly if there are outstanding Fate operations. Since Fate serializes class names, we
   * need to make sure there are no queued transactions from a previous version before continuing an
   * upgrade. The status of the operations is irrelevant; those in SUCCESSFUL status cause the same
   * problem as those just queued.
   * <p>
   * Note that the Manager should not allow write access to Fate until after all upgrade steps are
   * complete.
   * <p>
   * Should be called as a guard before performing any upgrade steps, after determining that an
   * upgrade is needed.
   * <p>
   * see ACCUMULO-2519
   */
  @SuppressFBWarnings(value = "DM_EXIT",
      justification = "Want to immediately stop all manager threads on upgrade error")
  private void abortIfFateTransactions(ServerContext context) {
    try {
      final ReadOnlyFateStore<UpgradeCoordinator> fate = new ZooStore<>(
          context.getZooKeeperRoot() + Constants.ZFATE, context.getZooReaderWriter());
      try (var idStream = fate.list()) {
        if (idStream.findFirst().isPresent()) {
          throw new AccumuloException("Aborting upgrade because there are"
              + " outstanding FATE transactions from a previous Accumulo version."
              + " You can start the tservers and then use the shell to delete completed "
              + " transactions. If there are incomplete transactions, you will need to roll"
              + " back and fix those issues. Please see the following page for more information: "
              + " https://accumulo.apache.org/docs/2.x/troubleshooting/advanced#upgrade-issues");
        }
      }
    } catch (Exception exception) {
      log.error("Problem verifying Fate readiness", exception);
      System.exit(1);
    }
  }
}
