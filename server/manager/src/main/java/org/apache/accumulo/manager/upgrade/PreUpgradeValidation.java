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

import org.apache.accumulo.core.zookeeper.ZooSession;
import org.apache.accumulo.core.zookeeper.ZooSession.ZKUtil;
import org.apache.accumulo.server.AccumuloDataVersion;
import org.apache.accumulo.server.ServerContext;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Provide checks before upgraders run that can perform checks that the environment from previous
 * versions match expectations. Checks include:
 * <ul>
 * <li>ACL validation of ZooKeeper nodes</li>
 * </ul>
 */
public class PreUpgradeValidation {

  private final static Logger log = LoggerFactory.getLogger(PreUpgradeValidation.class);

  public void validate(final ServerContext context) {
    int storedVersion = AccumuloDataVersion.getCurrentVersion(context);
    int currentVersion = AccumuloDataVersion.get();
    if (storedVersion == currentVersion) {
      log.debug("already at current data version: {}, skipping validation", currentVersion);
      return;
    }
    validateACLs(context);
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

  @SuppressFBWarnings(value = "DM_EXIT",
      justification = "Want to immediately stop all threads on upgrade error")
  private void fail(Exception e) {
    log.error("FATAL: Error performing pre-upgrade checks", e);
    System.exit(1);
  }

}
