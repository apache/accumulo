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
package org.apache.accumulo.server.zookeeper;

import static java.util.Objects.requireNonNull;

import java.util.List;

import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooAclUtil {

  private static final Logger log = LoggerFactory.getLogger(ZooAclUtil.class);

  /**
   * Utility class - prevent instantiation.
   */
  private ZooAclUtil() {}

  public static String extractAuthName(ACL acl) {
    requireNonNull(acl, "provided ACL cannot be null");
    try {
      return acl.getId().getId().trim().split(":")[0];
    } catch (Exception ex) {
      log.debug("Invalid ACL passed, cannot parse id from '{}'", acl);
      return "";
    }
  }

  /**
   * translate the ZooKeeper ACL perm bits into a string. The output order is cdrwa (when all perms
   * are set) Copied from `org.apache.zookeeper.ZKUtil.getPermString()` added in more recent
   * ZooKeeper versions.
   */
  public static String translateZooPerm(final int perm) {
    if (perm == ZooDefs.Perms.ALL) {
      return "cdrwa";
    }
    StringBuilder sb = new StringBuilder();
    if ((perm & ZooDefs.Perms.CREATE) != 0) {
      sb.append("c");
    }
    if ((perm & ZooDefs.Perms.DELETE) != 0) {
      sb.append("d");
    }
    if ((perm & ZooDefs.Perms.READ) != 0) {
      sb.append("r");
    }
    if ((perm & ZooDefs.Perms.WRITE) != 0) {
      sb.append("w");
    }
    if ((perm & ZooDefs.Perms.ADMIN) != 0) {
      sb.append("a");
    }
    if (sb.length() == 0) {
      return "invalid";
    }
    return sb.toString();
  }

  /**
   * Process the ZooKeeper acls and return a structure that shows if the accumulo user has ALL
   * (cdrwa) access and if any other user has anything other than read access.
   * <ul>
   * <li>Accumulo having access is expected - anything else needs checked
   * <li>Anyone having more than read access is unexpected and should be checked.
   * </ul>
   *
   * @param acls ZooKeeper ACLs for a node
   * @return acl status with accumulo and other accesses.
   */
  public static ZkAccumuloAclStatus checkWritableAuth(List<ACL> acls) {

    ZkAccumuloAclStatus result = new ZkAccumuloAclStatus();

    for (ACL a : acls) {
      String name = extractAuthName(a);
      // check accumulo has or inherits all access
      if ("accumulo".equals(name) || "anyone".equals(name)) {
        if (a.getPerms() == ZooDefs.Perms.ALL) {
          result.setAccumuloHasFull();
        }
      }
      // check if not accumulo and any perm bit other than read is set
      if (!"accumulo".equals(name) && ((a.getPerms() & ~ZooDefs.Perms.READ) != 0)) {
        result.setOthersMayUpdate();
      }
      // check if not accumulo and any read perm is set
      if (!"accumulo".equals(name) && ((a.getPerms() & ZooDefs.Perms.READ) != 0)) {
        result.setAnyCanRead();
      }
    }
    return result;
  }

  /**
   * Wrapper for decoding ZooKeeper ACLs in context of Accumulo usages. The expected usage is this
   * will be used by Accumulo utilities that print or check permissions of ZooKeeper nodes created /
   * owned by Accumulo.
   * <ul>
   * <li>It is expected that the authenticated Accumulo session will have cdrwa (ALL ZooKeeper
   * permissions). {@link #accumuloHasFull() accumuloHasFull()} should true.
   * <li>Typically, no other users should have permissions to modify any node in ZooKeeper under the
   * /accumulo path except for /accumulo/instances path {@link #othersMayUpdate() othersMayUpdate()}
   * return false.
   * <li>Most ZooKeeper nodes permit world read - a few are sensitive and are accessible only by the
   * authenticated Accumulo sessions. {@link #anyCanRead() anyCanRead()} returns true.
   * </ul>
   */
  public static class ZkAccumuloAclStatus {
    private boolean accumuloHasFull = false;
    private boolean othersMayUpdate = false;
    private boolean anyCanRead = false;

    public boolean accumuloHasFull() {
      return accumuloHasFull;
    }

    public void setAccumuloHasFull() {
      this.accumuloHasFull = true;
    }

    public boolean othersMayUpdate() {
      return othersMayUpdate;
    }

    public void setOthersMayUpdate() {
      this.othersMayUpdate = true;
    }

    public boolean anyCanRead() {
      return anyCanRead;
    }

    public void setAnyCanRead() {
      this.anyCanRead = true;
    }

    @Override
    public String toString() {
      return "ZkAccumuloAclStatus{accumuloHasFull=" + accumuloHasFull + ", anyMayUpdate="
          + othersMayUpdate + ", anyCanRead=" + anyCanRead + '}';
    }
  }

}
