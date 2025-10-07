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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Supplier;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.zookeeper.ZooSession;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooDefs.Perms;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;

public class ZooUtil {

  private static final Logger log = LoggerFactory.getLogger(ZooUtil.class);

  private ZooUtil() {}

  public enum NodeExistsPolicy {
    SKIP, OVERWRITE, FAIL
  }

  public enum NodeMissingPolicy {
    SKIP, CREATE, FAIL
  }

  public static class LockID {
    public final long eid;
    public final String path;
    public final String node;
    private final Supplier<String> serialized;

    public LockID(String path, String node, long eid) {
      // path must start with a '/', must not end with one, and must not contain '$'. These chars
      // would cause problems for serialization.
      Preconditions.checkArgument(
          path != null && !path.contains("$") && path.startsWith("/") && !path.endsWith("/"),
          "Illegal path %s", path);
      // node must not contain '$' or '/' because these chars would cause problems for
      // serialization.
      Preconditions.checkArgument(
          node != null && !node.contains("$") && !node.contains("/") && !node.isEmpty(),
          "Illegal node name %s", node);
      this.path = path;
      this.node = node;
      this.eid = eid;
      this.serialized = Suppliers.memoize(() -> path + "/" + node + "$" + Long.toHexString(eid));
    }

    /**
     * Returns serialized form of this object that can be deserialized using
     * {@link #deserialize(String)}
     */
    public String serialize() {
      return serialized.get();
    }

    /**
     * Deserializes a lock id created by {@link #serialize()}
     */
    public static LockID deserialize(String serializedLID) {
      String[] sa = serializedLID.split("\\$");
      int lastSlash = sa[0].lastIndexOf('/');

      if (sa.length != 2 || lastSlash < 0) {
        throw new IllegalArgumentException("Malformed serialized lock id " + serializedLID);
      }

      return new LockID(sa[0].substring(0, lastSlash), sa[0].substring(lastSlash + 1),
          Long.parseUnsignedLong(sa[1], 16));
    }

    @Override
    public String toString() {
      return "path = " + path + " node = " + node + " eid = " + Long.toHexString(eid);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }
      if (obj instanceof LockID other) {
        return this.path.equals(other.path) && this.node.equals(other.node)
            && this.eid == other.eid;
      }
      return false;
    }

    @Override
    public int hashCode() {
      return Objects.hash(path, node, eid);
    }
  }

  // Need to use Collections.unmodifiableList() instead of List.of() or List.copyOf(), because
  // ImmutableCollections.contains() doesn't handle nulls properly (JDK-8265905) and ZooKeeper (as
  // of 3.8.1) calls acl.contains((Object) null) which throws a NPE when passed an immutable
  // collection
  public static final List<ACL> PRIVATE =
      Collections.unmodifiableList(new ArrayList<>(Ids.CREATOR_ALL_ACL));

  public static final List<ACL> PUBLIC;
  static {
    var publicTmp = new ArrayList<>(PRIVATE);
    publicTmp.add(new ACL(Perms.READ, Ids.ANYONE_ID_UNSAFE));
    PUBLIC = Collections.unmodifiableList(publicTmp);
  }

  public static String getRoot(final InstanceId instanceId) {
    return Constants.ZROOT + "/" + instanceId.canonical();
  }

  /**
   * This method will delete a node and all its children.
   */
  public static void recursiveDelete(ZooSession zooKeeper, String zPath, NodeMissingPolicy policy)
      throws KeeperException, InterruptedException {
    if (policy == NodeMissingPolicy.CREATE) {
      throw new IllegalArgumentException(policy.name() + " is invalid for this operation");
    }
    try {
      // delete children
      for (String child : zooKeeper.getChildren(zPath, null)) {
        recursiveDelete(zooKeeper, zPath + "/" + child, NodeMissingPolicy.SKIP);
      }

      // delete self
      zooKeeper.delete(zPath, -1);
    } catch (KeeperException e) {
      // new child appeared; try again
      if (e.code() == Code.NOTEMPTY) {
        recursiveDelete(zooKeeper, zPath, policy);
      }
      if (policy == NodeMissingPolicy.SKIP && e.code() == Code.NONODE) {
        return;
      }
      throw e;
    }
  }

  public static String getInstanceName(ZooSession zk, InstanceId instanceId) {
    requireNonNull(zk);
    var instanceIdBytes = requireNonNull(instanceId).canonical().getBytes(UTF_8);
    for (String name : getInstanceNames(zk)) {
      var bytes = getInstanceIdBytesFromName(zk, name);
      if (Arrays.equals(bytes, instanceIdBytes)) {
        return name;
      }
    }
    return null;
  }

  private static List<String> getInstanceNames(ZooSession zk) {
    try {
      return zk.asReader().getChildren(Constants.ZROOT + Constants.ZINSTANCES);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted reading instance names from ZooKeeper", e);
    } catch (KeeperException e) {
      throw new IllegalStateException("Failed to read instance names from ZooKeeper", e);
    }
  }

  private static byte[] getInstanceIdBytesFromName(ZooSession zk, String name) {
    try {
      return zk.asReader()
          .getData(Constants.ZROOT + Constants.ZINSTANCES + "/" + requireNonNull(name));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(
          "Interrupted reading InstanceId from ZooKeeper for instance named " + name, e);
    } catch (KeeperException e) {
      log.warn("Failed to read InstanceId from ZooKeeper for instance named {}", name, e);
      return null;
    }
  }

  public static Map<String,InstanceId> getInstanceMap(ZooSession zk) {
    Map<String,InstanceId> idMap = new TreeMap<>();
    getInstanceNames(zk).forEach(name -> {
      byte[] instanceId = getInstanceIdBytesFromName(zk, name);
      if (instanceId != null) {
        idMap.put(name, InstanceId.of(new String(instanceId, UTF_8)));
      }
    });
    return idMap;
  }

  public static InstanceId getInstanceId(ZooSession zk, String name) {
    byte[] data = getInstanceIdBytesFromName(zk, name);
    if (data == null) {
      throw new IllegalStateException("Instance name " + name + " does not exist in ZooKeeper. "
          + "Run \"accumulo org.apache.accumulo.server.util.ListInstances\" to see a list.");
    }
    String instanceIdString = new String(data, UTF_8);
    try {
      // verify that the instanceId found via the name actually exists
      if (zk.asReader().getData(Constants.ZROOT + "/" + instanceIdString) == null) {
        throw new IllegalStateException("InstanceId " + instanceIdString
            + " pointed to by the name " + name + " does not exist in ZooKeeper");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted verifying InstanceId " + instanceIdString
          + " pointed to by instance named " + name + " actually exists in ZooKeeper", e);
    } catch (KeeperException e) {
      throw new IllegalStateException("Failed to verify InstanceId " + instanceIdString
          + " pointed to by instance named " + name + " actually exists in ZooKeeper", e);
    }
    return InstanceId.of(instanceIdString);
  }

}
