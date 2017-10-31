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
package org.apache.accumulo.fate.zookeeper;

import static java.util.Objects.requireNonNull;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooDefs.Perms;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooUtil {
  private static final Logger log = LoggerFactory.getLogger(ZooUtil.class);

  public enum NodeExistsPolicy {
    SKIP, OVERWRITE, FAIL
  }

  public enum NodeMissingPolicy {
    SKIP, CREATE, FAIL
  }

  public static class LockID {
    public long eid;
    public String path;
    public String node;

    public LockID(String root, String serializedLID) {
      String sa[] = serializedLID.split("\\$");
      int lastSlash = sa[0].lastIndexOf('/');

      if (sa.length != 2 || lastSlash < 0) {
        throw new IllegalArgumentException("Malformed serialized lock id " + serializedLID);
      }

      if (lastSlash == 0)
        path = root;
      else
        path = root + "/" + sa[0].substring(0, lastSlash);
      node = sa[0].substring(lastSlash + 1);
      eid = new BigInteger(sa[1], 16).longValue();
    }

    public LockID(String path, String node, long eid) {
      this.path = path;
      this.node = node;
      this.eid = eid;
    }

    public String serialize(String root) {

      return path.substring(root.length()) + "/" + node + "$" + Long.toHexString(eid);
    }

    @Override
    public String toString() {
      return " path = " + path + " node = " + node + " eid = " + Long.toHexString(eid);
    }
  }

  protected static class ZooKeeperConnectionInfo {
    String keepers, scheme;
    int timeout;
    byte[] auth;

    public ZooKeeperConnectionInfo(String keepers, int timeout, String scheme, byte[] auth) {
      requireNonNull(keepers);
      this.keepers = keepers;
      this.timeout = timeout;
      this.scheme = scheme;
      this.auth = auth;
    }

    @Override
    public int hashCode() {
      final HashCodeBuilder hcb = new HashCodeBuilder(31, 47);
      hcb.append(keepers).append(timeout);
      if (null != scheme) {
        hcb.append(scheme);
      }
      if (null != auth) {
        hcb.append(auth);
      }
      return hcb.toHashCode();
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof ZooKeeperConnectionInfo) {
        ZooKeeperConnectionInfo other = (ZooKeeperConnectionInfo) o;
        if (!keepers.equals(other.keepers) || timeout != other.timeout) {
          return false;
        }

        if (null != scheme) {
          if (null == other.scheme) {
            // Ours is non-null, theirs is null
            return false;
          } else if (!scheme.equals(other.scheme)) {
            // Both non-null but not equal
            return false;
          }
        }

        if (null != auth) {
          if (null == other.auth) {
            return false;
          } else if (!Arrays.equals(auth, other.auth)) {
            // both non-null but not equal
            return false;
          }
        }

        return true;
      }

      return false;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder(64);
      sb.append("zookeepers=").append(keepers);
      sb.append(", timeout=").append(timeout);
      sb.append(", scheme=").append(scheme);
      sb.append(", auth=").append(null == auth ? "null" : "REDACTED");
      return sb.toString();
    }
  }

  public static final List<ACL> PRIVATE;
  public static final List<ACL> PUBLIC;
  private static final RetryFactory RETRY_FACTORY;

  static {
    PRIVATE = new ArrayList<>();
    PRIVATE.addAll(Ids.CREATOR_ALL_ACL);
    PUBLIC = new ArrayList<>();
    PUBLIC.addAll(PRIVATE);
    PUBLIC.add(new ACL(Perms.READ, Ids.ANYONE_ID_UNSAFE));
    RETRY_FACTORY = RetryFactory.DEFAULT_INSTANCE;
  }

  protected static ZooKeeper getZooKeeper(ZooKeeperConnectionInfo info) {
    return getZooKeeper(info.keepers, info.timeout, info.scheme, info.auth);
  }

  protected static ZooKeeper getZooKeeper(String keepers, int timeout, String scheme, byte[] auth) {
    return ZooSession.getSession(keepers, timeout, scheme, auth);
  }

  protected static void retryOrThrow(Retry retry, KeeperException e) throws KeeperException {
    log.warn("Saw (possibly) transient exception communicating with ZooKeeper", e);
    if (retry.canRetry()) {
      retry.useRetry();
      return;
    }

    log.error("Retry attempts ({}) exceeded trying to communicate with ZooKeeper", retry.retriesCompleted());
    throw e;
  }

  /**
   * This method will delete a node and all its children from zookeeper
   *
   * @param zPath
   *          the path to delete
   */
  static void recursiveDelete(ZooKeeperConnectionInfo info, String zPath, NodeMissingPolicy policy) throws KeeperException, InterruptedException {
    if (policy.equals(NodeMissingPolicy.CREATE))
      throw new IllegalArgumentException(policy.name() + " is invalid for this operation");
    try {
      List<String> children;
      final Retry retry = RETRY_FACTORY.create();
      while (true) {
        try {
          children = getZooKeeper(info).getChildren(zPath, false);
          break;
        } catch (KeeperException e) {
          final Code c = e.code();
          if (c == Code.CONNECTIONLOSS || c == Code.OPERATIONTIMEOUT || c == Code.SESSIONEXPIRED) {
            retryOrThrow(retry, e);
          } else {
            throw e;
          }
        }
        retry.waitForNextAttempt();
      }
      for (String child : children)
        recursiveDelete(info, zPath + "/" + child, NodeMissingPolicy.SKIP);

      Stat stat;
      while (true) {
        try {
          stat = getZooKeeper(info).exists(zPath, null);
          // Node exists
          if (stat != null) {
            try {
              // Try to delete it. We don't care if there was an update to the node
              // since we got the Stat, just delete all versions (-1).
              getZooKeeper(info).delete(zPath, -1);
              return;
            } catch (NoNodeException e) {
              // If the node is gone now, it's ok if we have SKIP
              if (policy.equals(NodeMissingPolicy.SKIP)) {
                return;
              }
              throw e;
            }
            // Let other KeeperException bubble to the outer catch
          } else {
            // If the stat is null, the node is now gone which is fine.
            return;
          }
        } catch (KeeperException e) {
          final Code c = e.code();
          if (c == Code.CONNECTIONLOSS || c == Code.OPERATIONTIMEOUT || c == Code.SESSIONEXPIRED) {
            retryOrThrow(retry, e);
          } else {
            throw e;
          }
        }

        retry.waitForNextAttempt();
      }
    } catch (KeeperException e) {
      if (policy.equals(NodeMissingPolicy.SKIP) && e.code().equals(KeeperException.Code.NONODE))
        return;
      throw e;
    }
  }

  /**
   * Create a persistent node with the default ACL
   *
   * @return true if the node was created or altered; false if it was skipped
   */
  public static boolean putPersistentData(ZooKeeperConnectionInfo info, String zPath, byte[] data, NodeExistsPolicy policy) throws KeeperException,
      InterruptedException {
    return putData(info, zPath, data, CreateMode.PERSISTENT, -1, policy, PUBLIC);
  }

  public static boolean putPersistentData(ZooKeeperConnectionInfo info, String zPath, byte[] data, int version, NodeExistsPolicy policy)
      throws KeeperException, InterruptedException {
    return putData(info, zPath, data, CreateMode.PERSISTENT, version, policy, PUBLIC);
  }

  public static boolean putPersistentData(ZooKeeperConnectionInfo info, String zPath, byte[] data, int version, NodeExistsPolicy policy, List<ACL> acls)
      throws KeeperException, InterruptedException {
    return putData(info, zPath, data, CreateMode.PERSISTENT, version, policy, acls);
  }

  private static boolean putData(ZooKeeperConnectionInfo info, String zPath, byte[] data, CreateMode mode, int version, NodeExistsPolicy policy, List<ACL> acls)
      throws KeeperException, InterruptedException {
    if (policy == null)
      policy = NodeExistsPolicy.FAIL;

    final Retry retry = RETRY_FACTORY.create();
    while (true) {
      try {
        getZooKeeper(info).create(zPath, data, acls, mode);
        return true;
      } catch (KeeperException e) {
        final Code code = e.code();
        if (code == Code.NODEEXISTS) {
          switch (policy) {
            case SKIP:
              return false;
            case OVERWRITE:
              // overwrite the data in the node when it already exists
              try {
                getZooKeeper(info).setData(zPath, data, version);
                return true;
              } catch (KeeperException e2) {
                final Code code2 = e2.code();
                if (code2 == Code.NONODE) {
                  // node delete between create call and set data, so try create call again
                  continue;
                } else if (code2 == Code.CONNECTIONLOSS || code2 == Code.OPERATIONTIMEOUT || code2 == Code.SESSIONEXPIRED) {
                  retryOrThrow(retry, e2);
                  break;
                } else {
                  // unhandled exception on setData()
                  throw e2;
                }
              }
            default:
              throw e;
          }
        } else if (code == Code.CONNECTIONLOSS || code == Code.OPERATIONTIMEOUT || code == Code.SESSIONEXPIRED) {
          retryOrThrow(retry, e);
        } else {
          // unhandled exception on create()
          throw e;
        }
      }

      // Catch all to wait before retrying
      retry.waitForNextAttempt();
    }
  }

  public static byte[] getData(ZooKeeperConnectionInfo info, String zPath, Stat stat) throws KeeperException, InterruptedException {
    final Retry retry = RETRY_FACTORY.create();
    while (true) {
      try {
        return getZooKeeper(info).getData(zPath, false, stat);
      } catch (KeeperException e) {
        final Code c = e.code();
        if (c == Code.CONNECTIONLOSS || c == Code.OPERATIONTIMEOUT || c == Code.SESSIONEXPIRED) {
          retryOrThrow(retry, e);
        } else {
          throw e;
        }
      }

      retry.waitForNextAttempt();
    }
  }

  public static Stat getStatus(ZooKeeperConnectionInfo info, String zPath) throws KeeperException, InterruptedException {
    final Retry retry = RETRY_FACTORY.create();
    while (true) {
      try {
        return getZooKeeper(info).exists(zPath, false);
      } catch (KeeperException e) {
        final Code c = e.code();
        if (c == Code.CONNECTIONLOSS || c == Code.OPERATIONTIMEOUT || c == Code.SESSIONEXPIRED) {
          retryOrThrow(retry, e);
        } else {
          throw e;
        }
      }

      retry.waitForNextAttempt();
    }
  }

  public static boolean exists(ZooKeeperConnectionInfo info, String zPath) throws KeeperException, InterruptedException {
    return getStatus(info, zPath) != null;
  }

  public static void recursiveCopyPersistent(ZooKeeperConnectionInfo info, String source, String destination, NodeExistsPolicy policy) throws KeeperException,
      InterruptedException {
    Stat stat = null;
    if (!exists(info, source))
      throw KeeperException.create(Code.NONODE, source);
    if (exists(info, destination)) {
      switch (policy) {
        case OVERWRITE:
          break;
        case SKIP:
          return;
        case FAIL:
        default:
          throw KeeperException.create(Code.NODEEXISTS, source);
      }
    }

    stat = new Stat();
    byte[] data = getData(info, source, stat);

    if (stat.getEphemeralOwner() == 0) {
      if (data == null)
        throw KeeperException.create(Code.NONODE, source);
      putPersistentData(info, destination, data, policy);
      if (stat.getNumChildren() > 0) {
        List<String> children;
        final Retry retry = RETRY_FACTORY.create();
        while (true) {
          try {
            children = getZooKeeper(info).getChildren(source, false);
            break;
          } catch (KeeperException e) {
            final Code c = e.code();
            if (c == Code.CONNECTIONLOSS || c == Code.OPERATIONTIMEOUT || c == Code.SESSIONEXPIRED) {
              retryOrThrow(retry, e);
            } else {
              throw e;
            }
          }
          retry.waitForNextAttempt();
        }
        for (String child : children) {
          recursiveCopyPersistent(info, source + "/" + child, destination + "/" + child, policy);
        }
      }
    }
  }

  public static boolean putPrivatePersistentData(ZooKeeperConnectionInfo info, String zPath, byte[] data, NodeExistsPolicy policy) throws KeeperException,
      InterruptedException {
    return putData(info, zPath, data, CreateMode.PERSISTENT, -1, policy, PRIVATE);
  }

  public static String putPersistentSequential(ZooKeeperConnectionInfo info, String zPath, byte[] data) throws KeeperException, InterruptedException {
    final Retry retry = RETRY_FACTORY.create();
    while (true) {
      try {
        return getZooKeeper(info).create(zPath, data, ZooUtil.PUBLIC, CreateMode.PERSISTENT_SEQUENTIAL);
      } catch (KeeperException e) {
        final Code c = e.code();
        if (c == Code.CONNECTIONLOSS || c == Code.OPERATIONTIMEOUT || c == Code.SESSIONEXPIRED) {
          retryOrThrow(retry, e);
        } else {
          throw e;
        }
      }

      retry.waitForNextAttempt();
    }
  }

  public static String putEphemeralData(ZooKeeperConnectionInfo info, String zPath, byte[] data) throws KeeperException, InterruptedException {
    final Retry retry = RETRY_FACTORY.create();
    while (true) {
      try {
        return getZooKeeper(info).create(zPath, data, ZooUtil.PUBLIC, CreateMode.EPHEMERAL);
      } catch (KeeperException e) {
        final Code c = e.code();
        if (c == Code.CONNECTIONLOSS || c == Code.OPERATIONTIMEOUT || c == Code.SESSIONEXPIRED) {
          retryOrThrow(retry, e);
        } else {
          throw e;
        }
      }

      retry.waitForNextAttempt();
    }
  }

  public static String putEphemeralSequential(ZooKeeperConnectionInfo info, String zPath, byte[] data) throws KeeperException, InterruptedException {
    final Retry retry = RETRY_FACTORY.create();
    while (true) {
      try {
        return getZooKeeper(info).create(zPath, data, ZooUtil.PUBLIC, CreateMode.EPHEMERAL_SEQUENTIAL);
      } catch (KeeperException e) {
        final Code c = e.code();
        if (c == Code.CONNECTIONLOSS || c == Code.OPERATIONTIMEOUT || c == Code.SESSIONEXPIRED) {
          retryOrThrow(retry, e);
        } else {
          throw e;
        }
      }

      retry.waitForNextAttempt();
    }
  }

  public static byte[] getLockData(ZooCache zc, String path) {

    List<String> children = zc.getChildren(path);

    if (children == null || children.size() == 0) {
      return null;
    }

    children = new ArrayList<>(children);
    Collections.sort(children);

    String lockNode = children.get(0);

    return zc.get(path + "/" + lockNode);
  }

  public static boolean isLockHeld(ZooKeeperConnectionInfo info, LockID lid) throws KeeperException, InterruptedException {
    final Retry retry = RETRY_FACTORY.create();
    while (true) {
      try {
        List<String> children = getZooKeeper(info).getChildren(lid.path, false);

        if (children.size() == 0) {
          return false;
        }

        Collections.sort(children);

        String lockNode = children.get(0);
        if (!lid.node.equals(lockNode))
          return false;

        Stat stat = getZooKeeper(info).exists(lid.path + "/" + lid.node, false);
        return stat != null && stat.getEphemeralOwner() == lid.eid;
      } catch (KeeperException ex) {
        final Code c = ex.code();
        if (c == Code.CONNECTIONLOSS || c == Code.OPERATIONTIMEOUT || c == Code.SESSIONEXPIRED) {
          retryOrThrow(retry, ex);
        }
      }

      retry.waitForNextAttempt();
    }
  }

  public static List<ACL> getACL(ZooKeeperConnectionInfo info, String zPath, Stat stat) throws KeeperException, InterruptedException {
    final Retry retry = RETRY_FACTORY.create();
    while (true) {
      try {
        return getZooKeeper(info).getACL(zPath, stat);
      } catch (KeeperException e) {
        final Code c = e.code();
        if (c == Code.CONNECTIONLOSS || c == Code.OPERATIONTIMEOUT || c == Code.SESSIONEXPIRED) {
          retryOrThrow(retry, e);
        } else {
          throw e;
        }
      }

      retry.waitForNextAttempt();
    }
  }

}
