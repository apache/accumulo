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

import java.util.List;
import java.util.Objects;

import org.apache.accumulo.core.clientImpl.AcceptableThriftTableOperationException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

public class ZooReaderWriter extends ZooReader {
  public interface Mutator {
    byte[] mutate(byte[] currentValue) throws AcceptableThriftTableOperationException;
  }

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
    return retryLoop(zk -> zk.getACL(zPath, stat));
  }

  /**
   * Create a persistent node with the default ACL
   */
  public void putPersistentData(String zPath, byte[] data, NodeExistsPolicy policy)
      throws KeeperException, InterruptedException {
    putPersistentData(zPath, data, policy, ZooUtil.PUBLIC);
  }

  public void putPrivatePersistentData(String zPath, byte[] data, NodeExistsPolicy policy)
      throws KeeperException, InterruptedException {
    putPersistentData(zPath, data, policy, ZooUtil.PRIVATE);
  }

  public void putPersistentData(String zPath, byte[] data, NodeExistsPolicy policy, List<ACL> acls)
      throws KeeperException, InterruptedException {
    putData(zPath, data, CreateMode.PERSISTENT, policy, acls);
  }

  public String putPersistentSequential(String zPath, byte[] data)
      throws KeeperException, InterruptedException {
    return retryLoop(
        zk -> zk.create(zPath, data, ZooUtil.PUBLIC, CreateMode.PERSISTENT_SEQUENTIAL));
  }

  public String putEphemeralData(String zPath, byte[] data)
      throws KeeperException, InterruptedException {
    return retryLoop(zk -> zk.create(zPath, data, ZooUtil.PUBLIC, CreateMode.EPHEMERAL));
  }

  public String putEphemeralSequential(String zPath, byte[] data)
      throws KeeperException, InterruptedException {
    return retryLoop(zk -> zk.create(zPath, data, ZooUtil.PUBLIC, CreateMode.EPHEMERAL_SEQUENTIAL));
  }

  public void recursiveCopyPersistentOverwrite(String source, String destination)
      throws KeeperException, InterruptedException {
    var stat = new Stat();
    byte[] data = getData(source, stat);
    // only copy persistent data
    if (stat.getEphemeralOwner() != 0) {
      return;
    }
    putData(destination, data, CreateMode.PERSISTENT, NodeExistsPolicy.OVERWRITE, ZooUtil.PUBLIC);
    if (stat.getNumChildren() > 0) {
      for (String child : getChildren(source)) {
        recursiveCopyPersistentOverwrite(source + "/" + child, destination + "/" + child);
      }
    }
  }

  public byte[] mutate(String zPath, byte[] createValue, List<ACL> acl, Mutator mutator)
      throws KeeperException, InterruptedException, AcceptableThriftTableOperationException {
    if (createValue != null) {
      try {
        retryLoop(zk -> zk.create(zPath, createValue, acl, CreateMode.PERSISTENT));
        // create node was successful; return current (new) value
        return createValue;
      } catch (KeeperException e) {
        // if value already exists, use mutator instead
        if (e.code() != Code.NODEEXISTS) {
          throw e;
        }
      }
    }
    return retryLoopMutator(zk -> {
      var stat = new Stat();
      byte[] data = zk.getData(zPath, null, stat);
      // this mutator can throw AcceptableThriftTableOperationException
      data = mutator.mutate(data);
      if (data != null) {
        zk.setData(zPath, data, stat.getVersion());
      }
      return data;
    }, e -> e.code() == Code.BADVERSION); // always retry if bad version
  }

  public void mkdirs(String path) throws KeeperException, InterruptedException {
    if (path.equals("")) {
      // terminal condition for recursion
      return;
    }
    if (!path.startsWith("/")) {
      throw new IllegalArgumentException(path + "does not start with /");
    }
    if (exists(path)) {
      return;
    }
    String parent = path.substring(0, path.lastIndexOf("/"));
    mkdirs(parent);
    putPersistentData(path, new byte[0], NodeExistsPolicy.SKIP);
  }

  /**
   * Delete the specified node, and ignore NONODE exceptions.
   */
  public void delete(String path) throws KeeperException, InterruptedException {
    retryLoop(zk -> {
      try {
        zk.delete(path, -1);
      } catch (KeeperException e) {
        // ignore the case where the node doesn't exist
        if (e.code() != Code.NONODE) {
          throw e;
        }
      }
      return null;
    });
  }

  /**
   * This method will delete a node and all its children from zookeeper
   *
   * @param zPath
   *          the path to delete
   */
  public void recursiveDelete(String zPath, NodeMissingPolicy policy)
      throws KeeperException, InterruptedException {
    if (policy == NodeMissingPolicy.CREATE) {
      throw new IllegalArgumentException(policy.name() + " is invalid for this operation");
    }
    try {
      // delete children
      for (String child : getChildren(zPath)) {
        recursiveDelete(zPath + "/" + child, NodeMissingPolicy.SKIP);
      }

      // delete self
      retryLoop(zk -> {
        zk.delete(zPath, -1);
        return null;
      });
    } catch (KeeperException e) {
      // new child appeared; try again
      if (e.code() == Code.NOTEMPTY) {
        recursiveDelete(zPath, policy);
      }
      if (policy == NodeMissingPolicy.SKIP && e.code() == Code.NONODE) {
        return;
      }
      throw e;
    }
  }

  private void putData(String zPath, byte[] data, CreateMode mode, NodeExistsPolicy policy,
      List<ACL> acls) throws KeeperException, InterruptedException {
    Objects.requireNonNull(policy);
    retryLoop(zk -> {
      try {
        zk.create(zPath, data, acls, mode);
        return null;
      } catch (KeeperException e) {
        if (e.code() == Code.NODEEXISTS) {
          switch (policy) {
            case SKIP:
              return null;
            case OVERWRITE:
              zk.setData(zPath, data, -1);
              return null;
            default:
          }
        }
        throw e;
      }
    },
        // if OVERWRITE policy is used, create() can fail with NODEEXISTS;
        // then, the node can be deleted, causing setData() to fail with NONODE;
        // if that happens, the following code ensures we retry
        e -> e.code() == Code.NONODE && policy == NodeExistsPolicy.OVERWRITE);
  }
}
