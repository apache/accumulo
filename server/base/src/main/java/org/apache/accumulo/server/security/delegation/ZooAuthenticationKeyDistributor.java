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
package org.apache.accumulo.server.security.delegation;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.fate.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class that manages distribution of {@link AuthenticationKey}s, Accumulo's secret in the delegation token model, to other Accumulo nodes via ZooKeeper.
 */
public class ZooAuthenticationKeyDistributor {
  private static final Logger log = LoggerFactory.getLogger(ZooAuthenticationKeyDistributor.class);

  private final ZooReaderWriter zk;
  private final String baseNode;
  private AtomicBoolean initialized = new AtomicBoolean(false);

  public ZooAuthenticationKeyDistributor(ZooReaderWriter zk, String baseNode) {
    requireNonNull(zk);
    requireNonNull(baseNode);
    this.zk = zk;
    this.baseNode = baseNode;
  }

  /**
   * Ensures that ZooKeeper is in a correct state to perform distribution of {@link AuthenticationKey}s.
   */
  public synchronized void initialize() throws KeeperException, InterruptedException {
    if (initialized.get()) {
      return;
    }

    if (!zk.exists(baseNode)) {
      if (!zk.putPrivatePersistentData(baseNode, new byte[0], NodeExistsPolicy.FAIL)) {
        throw new AssertionError("Got false from putPrivatePersistentData method");
      }
    } else {
      List<ACL> acls = zk.getACL(baseNode, new Stat());
      if (1 == acls.size()) {
        ACL actualAcl = acls.get(0), expectedAcl = ZooUtil.PRIVATE.get(0);
        Id actualId = actualAcl.getId();
        // The expected outcome from ZooUtil.PRIVATE
        if (actualAcl.getPerms() == expectedAcl.getPerms() && actualId.getScheme().equals("digest") && actualId.getId().startsWith("accumulo:")) {
          initialized.set(true);
          return;
        }
      } else {
        log.error("Saw more than one ACL on the node");
      }

      log.error("Expected {} to have ACLs {} but was {}", baseNode, ZooUtil.PRIVATE, acls);
      throw new IllegalStateException("Delegation token secret key node in ZooKeeper is not protected.");
    }

    initialized.set(true);
  }

  /**
   * Fetch all {@link AuthenticationKey}s currently stored in ZooKeeper beneath the configured {@code baseNode}.
   *
   * @return A list of {@link AuthenticationKey}s
   */
  public List<AuthenticationKey> getCurrentKeys() throws KeeperException, InterruptedException {
    checkState(initialized.get(), "Not initialized");
    List<String> children = zk.getChildren(baseNode);

    // Shortcircuit to avoid a list creation
    if (children.isEmpty()) {
      return Collections.<AuthenticationKey> emptyList();
    }

    // Deserialize each byte[] into an AuthenticationKey
    List<AuthenticationKey> keys = new ArrayList<>(children.size());
    for (String child : children) {
      byte[] data = zk.getData(qualifyPath(child), null);
      if (null != data) {
        AuthenticationKey key = new AuthenticationKey();
        try {
          key.readFields(new DataInputStream(new ByteArrayInputStream(data)));
        } catch (IOException e) {
          throw new AssertionError("Error reading from in-memory buffer which should not happen", e);
        }
        keys.add(key);
      }
    }

    return keys;
  }

  /**
   * Add the given {@link AuthenticationKey} to ZooKeeper.
   *
   * @param newKey
   *          The key to add to ZooKeeper
   */
  public synchronized void advertise(AuthenticationKey newKey) throws KeeperException, InterruptedException {
    checkState(initialized.get(), "Not initialized");
    requireNonNull(newKey);

    // Make sure the node doesn't already exist
    String path = qualifyPath(newKey);
    if (zk.exists(path)) {
      log.warn("AuthenticationKey with ID '{}' already exists in ZooKeeper", newKey.getKeyId());
      return;
    }

    // Serialize it
    ByteArrayOutputStream baos = new ByteArrayOutputStream(4096);
    try {
      newKey.write(new DataOutputStream(baos));
    } catch (IOException e) {
      throw new AssertionError("Should not get exception writing to in-memory buffer", e);
    }

    byte[] serializedKey = baos.toByteArray();

    log.debug("Advertising AuthenticationKey with keyId {} in ZooKeeper at {}", newKey.getKeyId(), path);

    // Put it into ZK with the private ACL
    zk.putPrivatePersistentData(path, serializedKey, NodeExistsPolicy.FAIL);
  }

  /**
   * Remove the given {@link AuthenticationKey} from ZooKeeper. If the node for the provided {@code key} doesn't exist in ZooKeeper, a warning is printed but an
   * error is not thrown. Since there is only a single process managing ZooKeeper at one time, any inconsistencies should be client error.
   *
   * @param key
   *          The key to remove from ZooKeeper
   */
  public synchronized void remove(AuthenticationKey key) throws KeeperException, InterruptedException {
    checkState(initialized.get(), "Not initialized");
    requireNonNull(key);

    String path = qualifyPath(key);
    if (!zk.exists(path)) {
      log.warn("AuthenticationKey with ID '{}' doesn't exist in ZooKeeper", key.getKeyId());
      return;
    }

    log.debug("Removing AuthenticationKey with keyId {} from ZooKeeper at {}", key.getKeyId(), path);

    // Delete the node, any version
    zk.delete(path, -1);
  }

  String qualifyPath(String keyId) {
    return baseNode + "/" + keyId;
  }

  String qualifyPath(AuthenticationKey key) {
    return qualifyPath(Integer.toString(key.getKeyId()));
  }
}
