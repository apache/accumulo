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
package org.apache.accumulo.master.upgrade;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.master.EventCoordinator;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.ServerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class UpgradeCoordinator {

  public enum UpgradeStatus {
    /**
     * This signifies the upgrade status is in the process of being determined. Its best to assume
     * nothing is upgraded when seeing this.
     */
    INITIAL {
      @Override
      public boolean isParentLevelUpgraded(KeyExtent extent) {
        return false;
      }
    },
    /**
     * This signifies that only zookeeper has been upgraded so far.
     */
    UPGRADED_ZOOKEEPER {
      @Override
      public boolean isParentLevelUpgraded(KeyExtent extent) {
        return extent.isRootTablet();
      }
    },
    /**
     * This signifies that only zookeeper and the root table have been upgraded so far.
     */
    UPGRADED_ROOT {
      @Override
      public boolean isParentLevelUpgraded(KeyExtent extent) {
        return extent.isMeta();
      }
    },
    /**
     * This signifies that everything (zookeeper, root table, metadata table) is upgraded.
     */
    COMPLETE {
      @Override
      public boolean isParentLevelUpgraded(KeyExtent extent) {
        return true;
      }
    },
    /**
     * This signifies a failure occurred during upgrade.
     */
    FAILED {
      @Override
      public boolean isParentLevelUpgraded(KeyExtent extent) {
        return false;
      }
    };

    /**
     * Determines if the place where this extent stores its metadata was upgraded for a given
     * upgrade status.
     */
    public abstract boolean isParentLevelUpgraded(KeyExtent extent);
  }

  private static Logger log = LoggerFactory.getLogger(UpgradeCoordinator.class);

  private int currentVersion;
  private Map<Integer,Upgrader> upgraders = Map.of(ServerConstants.SHORTEN_RFILE_KEYS,
      new Upgrader8to9(), ServerConstants.CRYPTO_CHANGES, new Upgrader9to10());

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
      justification = "Want to immediately stop all master threads on upgrade error")
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
      int cv = ServerUtil.getAccumuloPersistentVersion(context.getVolumeManager());
      ServerUtil.ensureDataVersionCompatible(cv);
      this.currentVersion = cv;

      if (cv == ServerConstants.DATA_VERSION) {
        status = UpgradeStatus.COMPLETE;
        return;
      }

      if (currentVersion < ServerConstants.DATA_VERSION) {
        ServerUtil.abortIfFateTransactions(context);

        for (int v = currentVersion; v < ServerConstants.DATA_VERSION; v++) {
          log.info("Upgrading Zookeeper from data version {}", v);
          upgraders.get(v).upgradeZookeeper(context);
        }
      }

      setStatus(UpgradeStatus.UPGRADED_ZOOKEEPER, eventCoordinator);
    } catch (Exception e) {
      handleFailure(e);
    }

  }

  public synchronized Future<Void> upgradeMetadata(ServerContext context,
      EventCoordinator eventCoordinator) {
    if (status == UpgradeStatus.COMPLETE)
      return CompletableFuture.completedFuture(null);

    Preconditions.checkState(status == UpgradeStatus.UPGRADED_ZOOKEEPER,
        "Not currently in a suitable state to do metadata upgrade %s", status);

    if (currentVersion < ServerConstants.DATA_VERSION) {
      return Executors.newCachedThreadPool().submit(() -> {
        try {
          for (int v = currentVersion; v < ServerConstants.DATA_VERSION; v++) {
            log.info("Upgrading Root from data version {}", v);
            upgraders.get(v).upgradeRoot(context);
          }

          setStatus(UpgradeStatus.UPGRADED_ROOT, eventCoordinator);

          for (int v = currentVersion; v < ServerConstants.DATA_VERSION; v++) {
            log.info("Upgrading Metadata from data version {}", v);
            upgraders.get(v).upgradeMetadata(context);
          }

          log.info("Updating persistent data version.");
          ServerUtil.updateAccumuloVersion(context.getVolumeManager(), currentVersion);
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

  public UpgradeStatus getStatus() {
    return status;
  }
}
