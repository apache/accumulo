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

package org.apache.accumulo.master.upgrade;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.ServerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class UpgradeCoordinator {

  private static Logger log = LoggerFactory.getLogger(UpgradeCoordinator.class);

  private ServerContext context;
  private boolean haveUpgradedZooKeeper = false;
  private boolean startedMetadataUpgrade = false;
  private int currentVersion;
  private Map<Integer,Upgrader> upgraders =
      ImmutableMap.of(ServerConstants.SHORTEN_RFILE_KEYS, new Upgrader8to9());

  public UpgradeCoordinator(ServerContext ctx) {
    int currentVersion = ServerUtil.getAccumuloPersistentVersion(ctx.getVolumeManager());

    ServerUtil.ensureDataVersionCompatible(currentVersion);

    this.currentVersion = currentVersion;
    this.context = ctx;
  }

  @SuppressFBWarnings(value = "DM_EXIT",
      justification = "Want to immediately stop all master threads on upgrade error")
  private void handleFailure(Exception e) {
    log.error("FATAL: Error performing upgrade", e);
    System.exit(1);
  }

  public synchronized void upgradeZookeeper() {
    if (haveUpgradedZooKeeper)
      throw new IllegalStateException("Only expect this method to be called once");

    try {
      if (currentVersion < ServerConstants.DATA_VERSION) {
        ServerUtil.abortIfFateTransactions(context);

        for (int v = currentVersion; v < ServerConstants.DATA_VERSION; v++) {
          log.info("Upgrading Zookeeper from data version {}", v);
          upgraders.get(v).upgradeZookeeper(context);
        }
      }

      haveUpgradedZooKeeper = true;
    } catch (Exception e) {
      handleFailure(e);
    }
  }

  public synchronized Future<Void> upgradeMetadata() {
    if (startedMetadataUpgrade)
      throw new IllegalStateException("Only expect this method to be called once");

    if (!haveUpgradedZooKeeper) {
      throw new IllegalStateException("We should only attempt to upgrade"
          + " Accumulo's metadata table if we've already upgraded ZooKeeper."
          + " Please save all logs and file a bug.");
    }

    startedMetadataUpgrade = true;

    if (currentVersion < ServerConstants.DATA_VERSION) {
      return Executors.newCachedThreadPool().submit(() -> {
        try {
          for (int v = currentVersion; v < ServerConstants.DATA_VERSION; v++) {
            log.info("Upgrading Metadata from data version {}", v);
            upgraders.get(v).upgradeMetadata(context);
          }

          log.info("Updating persistent data version.");
          ServerUtil.updateAccumuloVersion(context.getVolumeManager(), currentVersion);
          log.info("Upgrade complete");
        } catch (Exception e) {
          handleFailure(e);
        }
        return null;
      });
    } else {
      return CompletableFuture.completedFuture(null);
    }
  }
}
