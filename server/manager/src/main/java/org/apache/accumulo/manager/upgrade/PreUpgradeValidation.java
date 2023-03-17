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

import static java.util.concurrent.TimeUnit.MINUTES;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.lock.ServiceLockData;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.manager.EventCoordinator;
import org.apache.accumulo.server.AccumuloDataVersion;
import org.apache.accumulo.server.ServerContext;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.net.HostAndPort;

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

  public void validate(final ServerContext context, final EventCoordinator eventCoordinator) {
    int cv = AccumuloDataVersion.getCurrentVersion(context);
    if (cv == AccumuloDataVersion.get()) {
      log.debug("already at current data version: {}, skipping validation", cv);
      return;
    }
    log.debug("Starting pre-upgrade validation checks.");

    validateACLs(context);
    validateTableLocks(context);

    log.debug("Completed pre-upgrade validation checks.");
  }

  private void validateACLs(ServerContext context) {

    final AtomicBoolean aclErrorOccurred = new AtomicBoolean(false);
    final ZooReaderWriter zrw = context.getZooReaderWriter();
    final ZooKeeper zk = zrw.getZooKeeper();
    final String rootPath = context.getZooKeeperRoot();
    final Set<String> users = Set.of("accumulo", "anyone");

    log.debug("Starting validation on ZooKeeper ACLs");

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
        fail(new RuntimeException(
            "Upgrade precondition failed! ACLs will need to be modified for some ZooKeeper nodes. "
                + "Check the log for specific failed paths, check ZooKeeper troubleshooting in user documentation "
                + "for instructions on how to fix."));
      }
    } catch (KeeperException | InterruptedException e) {
      fail(new RuntimeException("Upgrade Failed! Error validating nodes under " + rootPath, e));
    }
    log.debug("Successfully completed validation on ZooKeeper ACLs");
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
  protected void fail(Exception e) {
    log.error("FATAL: Error performing pre-upgrade checks", e);
    System.exit(1);
  }

  private void validateTableLocks(final ServerContext context) {

    final ZooReaderWriter zrw = context.getZooReaderWriter();
    final ZooKeeper zk = zrw.getZooKeeper();
    final String rootPath = context.getZooKeeperRoot();
    final String tserverLockRoot = rootPath + Constants.ZTSERVERS;

    log.debug("Looking for locks that may be from previous version in path: {}", tserverLockRoot);

    List<Pair<HostAndPort,ServiceLock.ServiceLockPath>> hostsWithLocks =
        gatherLocks(zk, tserverLockRoot);

    int numCheckThreads = Math.max(32, Runtime.getRuntime().availableProcessors() - 2);
    ThreadPoolExecutor lockCheckPool = ThreadPools.getServerThreadPools().createThreadPool(8,
        numCheckThreads, 10, MINUTES, "update-lock-check", false);

    // try a thrift call to the hosts - hosts running previous versions will fail the call
    final Map<String,String> pathErrors = new ConcurrentHashMap<>(0);
    hostsWithLocks.forEach(h -> lockCheckPool.execute(() -> {
      HostAndPort host = h.getFirst();
      ServiceLock.ServiceLockPath lockPath = h.getSecond();
      try (TTransport transport = ThriftUtil.createTransport(host, context)) {
        log.trace("found valid lock at: {}", lockPath);
      } catch (TException ex) {
        log.debug("Could not establish a connection for to service holding lock. Deleting node: {}",
            lockPath, ex);
        try {
          zk.delete(lockPath.toString(), -1);
        } catch (KeeperException.NoNodeException e) {
          // ignore - node already gone.
        } catch (InterruptedException | KeeperException e) {
          // task will be terminated - ignore interrupt.
          pathErrors.put(lockPath.toString(), "");
        }
      }
    }));
    lockCheckPool.shutdown();
    try {
      // wait to all to finish
      if (!lockCheckPool.awaitTermination(10, MINUTES)) {
        log.warn(
            "Timed out waiting for lock check to finish - continuing, but tservers running prior versions may be present");
      }
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("interrupted validating service locks in ZooKeeper", ex);
    }
    if (pathErrors.size() == 0) {
      log.info("Completed tserver lock check with no lock path errors");
    } else {
      log.info(
          "Completed tserver lock check with error count of: {}. (Paths printed at debug level)",
          pathErrors.size());
      if (log.isDebugEnabled()) {
        log.debug("tserver lock paths that could not be processed: {}", new TreeMap<>(pathErrors));
      }
    }
  }

  private List<Pair<HostAndPort,ServiceLock.ServiceLockPath>> gatherLocks(final ZooKeeper zk,
      final String zkPathRoot) {
    List<Pair<HostAndPort,ServiceLock.ServiceLockPath>> hosts = new ArrayList<>();
    try {
      ZKUtil.visitSubTreeDFS(zk, zkPathRoot, false, (rc, path, ctx, name) -> {
        if (name.startsWith(ServiceLock.ZLOCK_PREFIX)) {
          log.trace("found lock at {}", path);
          try {
            Stat stat = new Stat();
            final var zLockPath = ServiceLock.path(path);
            byte[] lockData = zk.getData(zLockPath.toString(), false, stat);
            if (lockData == null || !validLockData(lockData, hosts, zLockPath)) {
              log.debug("Invalid lock data - trying to delete path: {}", zLockPath);
              zk.delete(zLockPath.toString(), stat.getVersion());
            }
          } catch (KeeperException.NoNodeException ex) {
            // empty - lock no longer exists.
          } catch (KeeperException ex) {
            log.trace("could not read lock from ZooKeeper for {}, skipping", path, ex);
          } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                "interrupted reading lock from ZooKeeper for path: " + path);
          }
        }
      });
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(
          "interrupted reading lock from ZooKeeper for path: " + zkPathRoot);
    } catch (KeeperException ex) {
      log.trace("could not read lock all locks from ZooKeeper for {}, lost list may be incomplete",
          zkPathRoot, ex);
    }
    return hosts;
  }

  /**
   * Decode the lock data and if valid add the host, port pair to the hosts map. If the data cannot
   * be read return false to indicate invalid data.
   */
  private boolean validLockData(final byte[] lockData,
      List<Pair<HostAndPort,ServiceLock.ServiceLockPath>> hosts,
      ServiceLock.ServiceLockPath zLockPath) {
    try {
      Optional<ServiceLockData> sld = ServiceLockData.parse(lockData);
      if (sld.isPresent()) {
        HostAndPort hostAndPort = sld.get().getAddress(ServiceLockData.ThriftService.TSERV);
        hosts.add(new Pair<>(hostAndPort, zLockPath));
      }
      return true;
    } catch (Exception ex) {
      log.trace("Invalid lock data for path: {}", zLockPath, ex);
    }
    return false;
  }
}
