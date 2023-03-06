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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ZooUtilTest {
  Logger log = LoggerFactory.getLogger(ZooUtilTest.class);

  @Test
  void checkUnmodifiable() throws Exception {
    assertTrue(validateACL(ZooUtil.PRIVATE));
    assertTrue(validateACL(ZooUtil.PUBLIC));
  }

  @Test
  public void checkImmutableAcl() throws Exception {

    final List<ACL> mutable = new ArrayList<>(ZooDefs.Ids.CREATOR_ALL_ACL);
    assertTrue(validateACL(mutable));

    // Replicates the acl check in ZooKeeper.java to show ZooKeeper will not accept an
    // ImmutableCollection for the ACL list. ZooKeeper (as of 3.8.1) calls
    // acl.contains((Object) null) which throws a NPE when passed an immutable collectionCallers
    // because the way ImmutableCollections.contains() handles nulls (JDK-8265905)
    try {
      final List<ACL> immutable = List.copyOf(ZooDefs.Ids.CREATOR_ALL_ACL);
      assertThrows(NullPointerException.class, () -> validateACL(immutable));
    } catch (Exception ex) {
      log.warn("validateAcls failed with exception", ex);
    }
  }

  // Copied from ZooKeeper 3.8.1 for stand-alone testing here
  // https://github.com/apache/zookeeper/blob/2e9c3f3ceda90aeb9380acc87b253bf7661b7794/zookeeper-server/src/main/java/org/apache/zookeeper/ZooKeeper.java#L3075/
  private boolean validateACL(List<ACL> acl) throws KeeperException.InvalidACLException {
    if (acl == null || acl.isEmpty() || acl.contains((Object) null)) {
      throw new KeeperException.InvalidACLException();
    }
    return true;
  }
}
