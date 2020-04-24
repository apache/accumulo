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
package org.apache.accumulo.fate.zookeeper;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Collections;
import java.util.List;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.fate.util.Retry;
import org.apache.accumulo.fate.util.Retry.RetryFactory;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooReaderWriter extends ZooReader {
  public interface Mutator {
    byte[] mutate(byte[] currentValue) throws Exception;
  }

  private static final Logger log = LoggerFactory.getLogger(ZooReaderWriter.class);

  public ZooReaderWriter(AccumuloConfiguration conf) {
    this(conf.get(Property.INSTANCE_ZK_HOST),
        (int) conf.getTimeInMillis(Property.INSTANCE_ZK_TIMEOUT),
        conf.get(Property.INSTANCE_SECRET));
  }

  private final byte[] auth;

  public ZooReaderWriter(String keepers, int timeoutInMillis, String secret) {
    super(keepers, timeoutInMillis);
    this.auth = ("accumulo" + ":" + secret).getBytes(UTF_8);
  }

  @Override
  public ZooKeeper getZooKeeper() {
    return ZooSession.getAuthenticatedSession(keepers, timeout, "digest", auth);
  }

  public List<ACL> getACL(String zPath, Stat stat) throws KeeperException, InterruptedException {
    final Retry retry = getRetryFactory().createRetry();
    while (true) {
      try {
        return getZooKeeper().getACL(zPath, stat);
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

  /**
   * This method will delete a node and all its children from zookeeper
   *
   * @param zPath
   *          the path to delete
   */
  public void recursiveDelete(String zPath, NodeMissingPolicy policy)
      throws KeeperException, InterruptedException {
    if (policy.equals(NodeMissingPolicy.CREATE))
      throw new IllegalArgumentException(policy.name() + " is invalid for this operation");
    try {
      List<String> children;
      final Retry retry = getRetryFactory().createRetry();
      while (true) {
        try {
          children = getZooKeeper().getChildren(zPath, false);
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
        recursiveDelete(zPath + "/" + child, NodeMissingPolicy.SKIP);

      Stat stat;
      while (true) {
        try {
          stat = getZooKeeper().exists(zPath, null);
          // Node exists
          if (stat != null) {
            try {
              // Try to delete it. We don't care if there was an update to the node
              // since we got the Stat, just delete all versions (-1).
              getZooKeeper().delete(zPath, -1);
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
  public boolean putPersistentData(String zPath, byte[] data, NodeExistsPolicy policy)
      throws KeeperException, InterruptedException {
    return putPersistentData(zPath, data, policy, ZooUtil.PUBLIC);
  }

  public boolean putPrivatePersistentData(String zPath, byte[] data, NodeExistsPolicy policy)
      throws KeeperException, InterruptedException {
    return putPersistentData(zPath, data, policy, ZooUtil.PRIVATE);
  }

  public boolean putPersistentData(String zPath, byte[] data, NodeExistsPolicy policy,
      List<ACL> acls) throws KeeperException, InterruptedException {
    return putData(getZooKeeper(), getRetryFactory(), zPath, data, CreateMode.PERSISTENT, policy,
        acls);
  }

  public String putPersistentSequential(String zPath, byte[] data)
      throws KeeperException, InterruptedException {
    final Retry retry = getRetryFactory().createRetry();
    while (true) {
      try {
        return getZooKeeper().create(zPath, data, ZooUtil.PUBLIC, CreateMode.PERSISTENT_SEQUENTIAL);
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

  public String putEphemeralData(String zPath, byte[] data)
      throws KeeperException, InterruptedException {
    final Retry retry = getRetryFactory().createRetry();
    while (true) {
      try {
        return getZooKeeper().create(zPath, data, ZooUtil.PUBLIC, CreateMode.EPHEMERAL);
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

  public String putEphemeralSequential(String zPath, byte[] data)
      throws KeeperException, InterruptedException {
    final Retry retry = getRetryFactory().createRetry();
    while (true) {
      try {
        return getZooKeeper().create(zPath, data, ZooUtil.PUBLIC, CreateMode.EPHEMERAL_SEQUENTIAL);
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

  public void recursiveCopyPersistent(String source, String destination, NodeExistsPolicy policy)
      throws KeeperException, InterruptedException {
    Stat stat = null;
    if (getStatus(getZooKeeper(), getRetryFactory(), source) == null)
      throw KeeperException.create(Code.NONODE, source);
    if (getStatus(getZooKeeper(), getRetryFactory(), destination) != null) {
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
    byte[] data = getData(getZooKeeper(), getRetryFactory(), source, stat);

    if (stat.getEphemeralOwner() == 0) {
      if (data == null)
        throw KeeperException.create(Code.NONODE, source);
      putData(getZooKeeper(), getRetryFactory(), destination, data, CreateMode.PERSISTENT, policy,
          ZooUtil.PUBLIC);
      if (stat.getNumChildren() > 0) {
        List<String> children;
        final Retry retry = getRetryFactory().createRetry();
        while (true) {
          try {
            children = getZooKeeper().getChildren(source, false);
            break;
          } catch (KeeperException e) {
            final Code c = e.code();
            if (c == Code.CONNECTIONLOSS || c == Code.OPERATIONTIMEOUT
                || c == Code.SESSIONEXPIRED) {
              retryOrThrow(retry, e);
            } else {
              throw e;
            }
          }
          retry.waitForNextAttempt();
        }
        for (String child : children) {
          recursiveCopyPersistent(source + "/" + child, destination + "/" + child, policy);
        }
      }
    }
  }

  private static Stat getStatus(ZooKeeper zk, RetryFactory retryFactory, String zPath)
      throws KeeperException, InterruptedException {
    final Retry retry = retryFactory.createRetry();
    while (true) {
      try {
        return zk.exists(zPath, false);
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

  private static byte[] getData(ZooKeeper zk, RetryFactory retryFactory, String zPath, Stat stat)
      throws KeeperException, InterruptedException {
    final Retry retry = retryFactory.createRetry();
    while (true) {
      try {
        return zk.getData(zPath, false, stat);
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

  public void delete(String path, int version) throws InterruptedException, KeeperException {
    final Retry retry = getRetryFactory().createRetry();
    while (true) {
      try {
        getZooKeeper().delete(path, version);
        return;
      } catch (KeeperException e) {
        final Code code = e.code();
        if (code == Code.NONODE) {
          if (retry.hasRetried()) {
            // A retried delete could have deleted the node, assume that was the case
            log.debug("Delete saw no node on a retry. Assuming node was deleted");
            return;
          }

          throw e;
        } else if (code == Code.CONNECTIONLOSS || code == Code.OPERATIONTIMEOUT
            || code == Code.SESSIONEXPIRED) {
          // retry if we have more attempts to do so
          retryOrThrow(retry, e);
        } else {
          throw e;
        }
      }

      retry.waitForNextAttempt();
    }
  }

  public byte[] mutate(String zPath, byte[] createValue, List<ACL> acl, Mutator mutator)
      throws Exception {
    if (createValue != null) {
      while (true) {
        final Retry retry = getRetryFactory().createRetry();
        try {
          getZooKeeper().create(zPath, createValue, acl, CreateMode.PERSISTENT);
          return createValue;
        } catch (KeeperException ex) {
          final Code code = ex.code();
          if (code == Code.NODEEXISTS) {
            // expected
            break;
          } else if (code == Code.OPERATIONTIMEOUT || code == Code.CONNECTIONLOSS
              || code == Code.SESSIONEXPIRED) {
            retryOrThrow(retry, ex);
          } else {
            throw ex;
          }
        }

        retry.waitForNextAttempt();
      }
    }
    do {
      final Retry retry = getRetryFactory().createRetry();
      Stat stat = new Stat();
      byte[] data = getData(zPath, stat);
      data = mutator.mutate(data);
      if (data == null)
        return data;
      try {
        getZooKeeper().setData(zPath, data, stat.getVersion());
        return data;
      } catch (KeeperException ex) {
        final Code code = ex.code();
        if (code == Code.BADVERSION) {
          // Retry, but don't increment. This makes it backwards compatible with the infinite
          // loop that previously happened. I'm not sure if that's really desirable though.
        } else if (code == Code.OPERATIONTIMEOUT || code == Code.CONNECTIONLOSS
            || code == Code.SESSIONEXPIRED) {
          retryOrThrow(retry, ex);
          retry.waitForNextAttempt();
        } else {
          throw ex;
        }
      }
    } while (true);
  }

  public boolean isLockHeld(ZooUtil.LockID lockID) throws KeeperException, InterruptedException {
    final Retry retry = getRetryFactory().createRetry();
    while (true) {
      try {
        List<String> children = getZooKeeper().getChildren(lockID.path, false);

        if (children.isEmpty()) {
          return false;
        }

        Collections.sort(children);

        String lockNode = children.get(0);
        if (!lockID.node.equals(lockNode))
          return false;

        Stat stat = getZooKeeper().exists(lockID.path + "/" + lockID.node, false);
        return stat != null && stat.getEphemeralOwner() == lockID.eid;
      } catch (KeeperException ex) {
        final Code c = ex.code();
        if (c == Code.CONNECTIONLOSS || c == Code.OPERATIONTIMEOUT || c == Code.SESSIONEXPIRED) {
          retryOrThrow(retry, ex);
        }
      }

      retry.waitForNextAttempt();
    }
  }

  public void mkdirs(String path) throws KeeperException, InterruptedException {
    if (path.equals(""))
      return;
    if (!path.startsWith("/"))
      throw new IllegalArgumentException(path + "does not start with /");
    if (exists(path))
      return;
    String parent = path.substring(0, path.lastIndexOf("/"));
    mkdirs(parent);
    putPersistentData(path, new byte[] {}, NodeExistsPolicy.SKIP);
  }

  private static boolean putData(ZooKeeper zk, RetryFactory retryFactory, String zPath, byte[] data,
      CreateMode mode, NodeExistsPolicy policy, List<ACL> acls)
      throws KeeperException, InterruptedException {
    if (policy == null)
      policy = NodeExistsPolicy.FAIL;

    final Retry retry = retryFactory.createRetry();
    while (true) {
      try {
        zk.create(zPath, data, acls, mode);
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
                zk.setData(zPath, data, -1);
                return true;
              } catch (KeeperException e2) {
                final Code code2 = e2.code();
                if (code2 == Code.NONODE) {
                  // node delete between create call and set data, so try create call again
                  continue;
                } else if (code2 == Code.CONNECTIONLOSS || code2 == Code.OPERATIONTIMEOUT
                    || code2 == Code.SESSIONEXPIRED) {
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
        } else if (code == Code.CONNECTIONLOSS || code == Code.OPERATIONTIMEOUT
            || code == Code.SESSIONEXPIRED) {
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

}
