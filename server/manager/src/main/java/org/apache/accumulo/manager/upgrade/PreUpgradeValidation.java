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

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.ConfigCheckUtil;
import org.apache.accumulo.core.zookeeper.ZooSession;
import org.apache.accumulo.core.zookeeper.ZooSession.ZKUtil;
import org.apache.accumulo.manager.EventCoordinator;
import org.apache.accumulo.server.AccumuloDataVersion;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.CheckCompactionConfig;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Provide checks before upgraders run that can perform checks that the environment from previous
 * versions match expectations. Checks include:
 * <ul>
 * <li>ACL validation of ZooKeeper nodes</li>
 * <li>No FATE transactions are present</li>
 * <li>All Configuration properties are valid</li>
 * </ul>
 */
public class PreUpgradeValidation {

  private final static Logger log = LoggerFactory.getLogger(PreUpgradeValidation.class);

  public void validate(final ServerContext context, final EventCoordinator eventCoordinator) {
    int cv = AccumuloDataVersion.getCurrentVersion(context);
    if (cv == AccumuloDataVersion.get()) {
      log.debug("already at current data version: {}, skipping validation", cv);
      return;
    }

    try {
      validateACLs(context);
      abortIfFateTransactions(context);
      validateProperties(context);
    } catch (Exception e) {
      fail(e);
    }
  }

  private void validateACLs(ServerContext context) {

    final AtomicBoolean aclErrorOccurred = new AtomicBoolean(false);
    final ZooSession zk = context.getZooSession();
    final String rootPath = context.getZooKeeperRoot();
    final Set<String> users = Set.of("accumulo", "anyone");

    log.info("Starting validation on ZooKeeper ACLs");

    try {
      ZKUtil.visitSubTreeDFS(zk, rootPath, false, (rc, path, ctx, name) -> {
        try {
          final List<ACL> acls = zk.getACL(path, new Stat());
          if (!hasAllPermissions(users, acls)) {
            log.error(
                "ZNode at {} does not have an ACL that allows accumulo to write to it. ZNode ACL will need to be modified. Current ACLs: {}",
                path, acls);
            aclErrorOccurred.set(true);
          }
        } catch (KeeperException | InterruptedException e) {
          log.error("Error getting ACL for path: {}", path, e);
          aclErrorOccurred.set(true);
        }
      });
      if (aclErrorOccurred.get()) {
        throw new RuntimeException(
            "Upgrade precondition failed! ACLs will need to be modified for some ZooKeeper nodes. "
                + "Check the log for specific failed paths, check ZooKeeper troubleshooting in user documentation "
                + "for instructions on how to fix.");
      }
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException("Upgrade Failed! Error validating nodes under " + rootPath, e);
    }
    log.info("Successfully completed validation on ZooKeeper ACLs");
  }

  private static boolean hasAllPermissions(final Set<String> users, final List<ACL> acls) {
    return acls.stream()
        .anyMatch(a -> users.contains(extractAuthName(a)) && a.getPerms() == ZooDefs.Perms.ALL);
  }

  private static String extractAuthName(ACL acl) {
    Objects.requireNonNull(acl, "provided ACL cannot be null");
    try {
      return acl.getId().getId().trim().split(":")[0];
    } catch (Exception ex) {
      log.debug("Invalid ACL passed, cannot parse id from '{}'", acl);
      return "";
    }
  }

  private static void validateProperties(ServerContext context) {
    log.info("Validating configuration properties.");
    ConfigCheckUtil.validate(context.getSiteConfiguration(), "site configuration");
    ConfigCheckUtil.validate(context.getConfiguration(), "system configuration");
    try {
      for (String ns : context.namespaceOperations().list()) {
        ConfigCheckUtil.validate(
            context.namespaceOperations().getNamespaceProperties(ns).entrySet(),
            ns + " namespace configuration");
      }
      for (String table : context.tableOperations().list()) {
        ConfigCheckUtil.validate(context.tableOperations().getTableProperties(table).entrySet(),
            table + " table configuration");
      }
    } catch (AccumuloException | AccumuloSecurityException | NamespaceNotFoundException
        | TableNotFoundException e) {
      throw new IllegalStateException("Error checking properties", e);
    }
    try {
      CheckCompactionConfig.validate(context.getConfiguration(), Level.INFO);
    } catch (RuntimeException | ReflectiveOperationException e) {
      throw new IllegalStateException("Error validating compaction configuration", e);
    }
    log.info("Configuration properties validated.");
  }

  /**
   * Since Fate serializes class names, we need to make sure there are no queued transactions from a
   * previous version before continuing an upgrade. The status of the operations is irrelevant;
   * those in SUCCESSFUL status cause the same problem as those just queued.
   * <p>
   * Note that the Manager should not allow write access to Fate until after all upgrade steps are
   * complete.
   * <p>
   * Should be called as a guard before performing any upgrade steps, after determining that an
   * upgrade is needed.
   * <p>
   * see ACCUMULO-2519
   */
  public static void abortIfFateTransactions(ServerContext context) {
    try {
      // The current version of the code creates the new accumulo.fate table on upgrade, so no
      // attempt is made to read it here. Attempting to read it this point would likely cause a hang
      // as tablets are not assigned when this is called. The Fate code is not used to read from
      // zookeeper below because the serialization format changed in zookeeper, that is why a direct
      // read is performed.
      if (!context.getZooSession().asReader()
          .getChildren(context.getZooKeeperRoot() + Constants.ZFATE).isEmpty()) {
        throw new AccumuloException("Aborting upgrade because there are"
            + " outstanding FATE transactions from a previous Accumulo version."
            + " You can start the tservers and then use the shell to delete completed "
            + " transactions. If there are incomplete transactions, you will need to roll"
            + " back and fix those issues. Please see the following page for more information: "
            + " https://accumulo.apache.org/docs/2.x/troubleshooting/advanced#upgrade-issues");
      }
    } catch (Exception exception) {
      log.error("Problem verifying Fate readiness", exception);
      throw new IllegalStateException("FATEs are present", exception);
    }
    log.info("Completed FATE validation. No FATEs present.");
  }

  @SuppressFBWarnings(value = "DM_EXIT",
      justification = "Want to immediately stop all threads on upgrade error")
  private void fail(Exception e) {
    log.error("FATAL: Error performing pre-upgrade checks", e);
    System.exit(1);
  }

}
