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

import java.math.BigInteger;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooDefs.Perms;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;

public class ZooUtil {

  public enum NodeExistsPolicy {
    SKIP, OVERWRITE, FAIL
  }

  public enum NodeMissingPolicy {
    SKIP, CREATE, FAIL
  }

  // used for zookeeper stat print formatting
  private static final DateTimeFormatter fmt =
      DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss 'UTC' yyyy");

  public static class LockID {
    public long eid;
    public String path;
    public String node;

    public LockID(String root, String serializedLID) {
      String[] sa = serializedLID.split("\\$");
      int lastSlash = sa[0].lastIndexOf('/');

      if (sa.length != 2 || lastSlash < 0) {
        throw new IllegalArgumentException("Malformed serialized lock id " + serializedLID);
      }

      if (lastSlash == 0) {
        path = root;
      } else {
        path = root + "/" + sa[0].substring(0, lastSlash);
      }
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

  public static final List<ACL> PRIVATE;
  public static final List<ACL> PUBLIC;

  static {
    PRIVATE = new ArrayList<>();
    PRIVATE.addAll(Ids.CREATOR_ALL_ACL);
    PUBLIC = new ArrayList<>();
    PUBLIC.addAll(PRIVATE);
    PUBLIC.add(new ACL(Perms.READ, Ids.ANYONE_ID_UNSAFE));
  }

  public static String getRoot(final InstanceId instanceId) {
    return Constants.ZROOT + "/" + instanceId;
  }

  /**
   * This method will delete a node and all its children.
   */
  public static void recursiveDelete(ZooKeeper zooKeeper, String zPath, NodeMissingPolicy policy)
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

  /**
   * For debug: print the ZooKeeper Stat with value labels for a more user friendly string. The
   * format matches the zookeeper cli stat command.
   *
   * @param stat Zookeeper Stat structure
   * @return a formatted string.
   */
  public static String printStat(final Stat stat) {

    if (stat == null) {
      return "null";
    }

    return "\ncZxid = " + String.format("0x%x", stat.getCzxid()) + "\nctime = "
        + getFmtTime(stat.getCtime()) + "\nmZxid = " + String.format("0x%x", stat.getMzxid())
        + "\nmtime = " + getFmtTime(stat.getMtime()) + "\npZxid = "
        + String.format("0x%x", stat.getPzxid()) + "\ncversion = " + stat.getCversion()
        + "\ndataVersion = " + stat.getVersion() + "\naclVersion = " + stat.getAversion()
        + "\nephemeralOwner = " + String.format("0x%x", stat.getEphemeralOwner())
        + "\ndataLength = " + stat.getDataLength() + "\nnumChildren = " + stat.getNumChildren();
  }

  private static String getFmtTime(final long epoch) {
    OffsetDateTime timestamp =
        OffsetDateTime.ofInstant(Instant.ofEpochMilli(epoch), ZoneOffset.UTC);
    return fmt.format(timestamp);
  }

  /**
   * Get the ZooKeeper digest based on the instance secret that is used within ZooKeeper for
   * authentication. This method is primary intended to be used to validate ZooKeeper ACLs. Use
   * {@link #digestAuth(ZooKeeper, String)} to add authorizations to ZooKeeper.
   */
  public static Id getZkDigestAuthId(final String secret) {
    try {
      final String scheme = "digest";
      String auth = DigestAuthenticationProvider.generateDigest("accumulo:" + secret);
      return new Id(scheme, auth);
    } catch (NoSuchAlgorithmException ex) {
      throw new IllegalArgumentException("Could not generate ZooKeeper digest string", ex);
    }
  }

  public static void digestAuth(ZooKeeper zoo, String secret) {
    auth(zoo, "digest", ("accumulo:" + secret).getBytes(UTF_8));
  }

  public static void auth(ZooKeeper zoo, String scheme, byte[] auth) {
    zoo.addAuthInfo(scheme, auth);
  }

}
