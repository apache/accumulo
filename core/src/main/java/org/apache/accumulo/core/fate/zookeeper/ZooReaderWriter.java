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
package org.apache.accumulo.core.fate.zookeeper;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

import java.util.List;

import org.apache.accumulo.core.clientImpl.AcceptableThriftTableOperationException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeMissingPolicy;
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

  private final String secret;
  private final byte[] auth;

  ZooReaderWriter(String keepers, int timeoutInMillis, String secret) {
    super(keepers, timeoutInMillis);
    this.secret = requireNonNull(secret);
    this.auth = ("accumulo:" + secret).getBytes(UTF_8);
  }

  @Override
  public ZooReaderWriter asWriter(String secret) {
    if (this.secret.equals(secret)) {
      return this;
    }
    return super.asWriter(secret);
  }

  @Override
  public ZooKeeper getZooKeeper() {
    return ZooSession.getAuthenticatedSession(keepers, timeout, "digest", auth);
  }

  /**
   * Retrieve the ACL list that was on the node
   */
  public List<ACL> getACL(String zPath) throws KeeperException, InterruptedException {
    return retryLoop(zk -> zk.getACL(zPath, null));
  }

  /**
   * Create a persistent node with the default ACL
   *
   * @return true if the data was set on a new node or overwritten, and false if an existing node
   *         was skipped
   */
  public boolean putPersistentData(String zPath, byte[] data, NodeExistsPolicy policy)
      throws KeeperException, InterruptedException {
    return putPersistentData(zPath, data, policy, ZooUtil.PUBLIC);
  }

  /**
   * Create a persistent node with the private ACL
   *
   * @return true if the data was set on a new node or overwritten, and false if an existing node
   *         was skipped
   */
  public boolean putPrivatePersistentData(String zPath, byte[] data, NodeExistsPolicy policy)
      throws KeeperException, InterruptedException {
    return putPersistentData(zPath, data, policy, ZooUtil.PRIVATE);
  }

  /**
   * Create a persistent node with the provided ACLs
   *
   * @return true if the data was set on a new node or overwritten, and false if an existing node
   *         was skipped
   */
  public boolean putPersistentData(String zPath, byte[] data, NodeExistsPolicy policy,
      List<ACL> acls) throws KeeperException, InterruptedException {
    // zk allows null ACLs, but it's probably a bug in Accumulo if we see it used in our code
    requireNonNull(acls);
    requireNonNull(policy);
    return retryLoop(zk -> {
      try {
        zk.create(zPath, data, acls, CreateMode.PERSISTENT);
        return true;
      } catch (KeeperException e) {
        if (e.code() == Code.NODEEXISTS) {
          switch (policy) {
            case SKIP:
              return false;
            case OVERWRITE:
              zk.setData(zPath, data, -1);
              return true;
            case FAIL:
            default:
              // re-throw below
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

  /**
   * Overwrite a persistent node if the data version matches.
   *
   * @param zPath the zookeeper path
   * @param data the byte array data
   * @param expectedVersion the expected data version of the zookeeper node.
   * @return true if the data was set, false if the version does not match expected.
   * @throws KeeperException if a KeeperException occurs (no node most likely)
   * @throws InterruptedException if the zookeeper write is interrupted.
   */
  public boolean overwritePersistentData(String zPath, byte[] data, final int expectedVersion)
      throws KeeperException, InterruptedException {
    return retryLoop(zk -> {
      try {
        zk.setData(zPath, data, expectedVersion);
        return true;
      } catch (KeeperException.BadVersionException ex) {
        return false;
      }
    });
  }

  /**
   * Create a persistent sequential node with the default ACL
   *
   * @return the actual path of the created node
   */
  public String putPersistentSequential(String zPath, byte[] data)
      throws KeeperException, InterruptedException {
    return retryLoop(
        zk -> zk.create(zPath, data, ZooUtil.PUBLIC, CreateMode.PERSISTENT_SEQUENTIAL));
  }

  /**
   * Create an ephemeral node with the default ACL
   */
  public void putEphemeralData(String zPath, byte[] data)
      throws KeeperException, InterruptedException {
    retryLoop(zk -> zk.create(zPath, data, ZooUtil.PUBLIC, CreateMode.EPHEMERAL));
  }

  /**
   * Create an ephemeral sequential node with the default ACL
   *
   * @return the actual path of the created node
   */
  public String putEphemeralSequential(String zPath, byte[] data)
      throws KeeperException, InterruptedException {
    return retryLoop(zk -> zk.create(zPath, data, ZooUtil.PUBLIC, CreateMode.EPHEMERAL_SEQUENTIAL));
  }

  /**
   * Recursively copy any persistent data from the source to the destination, using the default ACL
   * to create any missing nodes and skipping over any ephemeral data.
   */
  public void recursiveCopyPersistentOverwrite(String source, String destination)
      throws KeeperException, InterruptedException {
    var stat = new Stat();
    byte[] data = getData(source, stat);
    // only copy persistent data
    if (stat.getEphemeralOwner() != 0) {
      return;
    }
    putPersistentData(destination, data, NodeExistsPolicy.OVERWRITE);
    if (stat.getNumChildren() > 0) {
      for (String child : getChildren(source)) {
        recursiveCopyPersistentOverwrite(source + "/" + child, destination + "/" + child);
      }
    }
  }

  /**
   * Update an existing ZK node using the provided mutator function. If it's possible the node
   * doesn't exist yet, use {@link #mutateOrCreate(String, byte[], Mutator)} instead.
   *
   * @return the value set on the node
   */
  public byte[] mutateExisting(String zPath, Mutator mutator)
      throws KeeperException, InterruptedException, AcceptableThriftTableOperationException {
    requireNonNull(mutator);
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

  /**
   * Create a new {@link CreateMode#PERSISTENT} ZK node with the default ACL if it does not exist.
   * If it does already exist, then update it with the provided mutator function. If it is known to
   * exist already, use {@link #mutateExisting(String, Mutator)} instead.
   *
   * @return the value set on the node
   */
  public byte[] mutateOrCreate(String zPath, byte[] createValue, Mutator mutator)
      throws KeeperException, InterruptedException, AcceptableThriftTableOperationException {
    requireNonNull(mutator);
    return putPersistentData(zPath, createValue, NodeExistsPolicy.SKIP) ? createValue
        : mutateExisting(zPath, mutator);
  }

  /**
   * Ensure the provided path exists, using persistent nodes, empty data, and the default ACL for
   * any missing path elements.
   */
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
    try {
      deleteStrict(path, -1);
    } catch (KeeperException e) {
      if (e.code() != Code.NONODE) {
        throw e;
      }
    }
  }

  /**
   * Delete the specified node if the version matches the provided version. All underlying
   * exceptions are thrown back to the caller.
   *
   * @param path the path of the ZooKeeper node.
   * @param version the expected version of the ZooKeeper node.
   */
  public void deleteStrict(final String path, final int version)
      throws KeeperException, InterruptedException {
    retryLoop(zk -> {
      zk.delete(path, version);
      return null;
    });
  }

  /**
   * This method will delete a node and all its children.
   */
  public void recursiveDelete(String zPath, NodeMissingPolicy policy)
      throws KeeperException, InterruptedException {
    ZooUtil.recursiveDelete(getZooKeeper(), zPath, policy);
  }
}
