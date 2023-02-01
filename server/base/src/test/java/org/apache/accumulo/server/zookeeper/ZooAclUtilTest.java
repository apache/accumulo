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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.junit.jupiter.api.Test;

public class ZooAclUtilTest {

  @Test
  public void extractAuthNameTest() {
    assertThrows(NullPointerException.class, () -> ZooAclUtil.extractAuthName(null));
    assertEquals("",
        ZooAclUtil.extractAuthName(new ACL(ZooDefs.Perms.ALL, new Id("digest", null))));
    assertEquals("accumulo", ZooAclUtil
        .extractAuthName(new ACL(ZooDefs.Perms.ALL, new Id("digest", "accumulo:abcd123"))));
    assertEquals("noauth",
        ZooAclUtil.extractAuthName(new ACL(ZooDefs.Perms.ALL, new Id("digest", "noauth"))));
    assertEquals("noscheme",
        ZooAclUtil.extractAuthName(new ACL(ZooDefs.Perms.ALL, new Id("", "noscheme"))));
    assertEquals("", ZooAclUtil.extractAuthName(new ACL(ZooDefs.Perms.ALL, new Id("digest", ""))));
  }

  @Test
  public void translateZooPermTest() {
    assertEquals("invalid", ZooAclUtil.translateZooPerm(0));
    assertEquals("cdrwa", ZooAclUtil.translateZooPerm(ZooDefs.Perms.ALL));
    assertEquals("drw", ZooAclUtil
        .translateZooPerm(ZooDefs.Perms.DELETE | ZooDefs.Perms.READ | ZooDefs.Perms.WRITE));
    assertEquals("ca", ZooAclUtil.translateZooPerm(ZooDefs.Perms.CREATE | ZooDefs.Perms.ADMIN));
  }

  @Test
  public void checkWritableAuthTest() {
    ACL a1 = new ACL(ZooDefs.Perms.ALL, new Id("digest", "accumulo:abcd123"));
    ZooAclUtil.ZkAccumuloAclStatus status = ZooAclUtil.checkWritableAuth(List.of(a1));
    assertTrue(status.accumuloHasFull());
    assertFalse(status.othersMayUpdate());
    assertFalse(status.anyCanRead());

    ACL a2 = new ACL(ZooDefs.Perms.ALL, new Id("digest", "someone:abcd123"));
    status = ZooAclUtil.checkWritableAuth(List.of(a2));
    assertFalse(status.accumuloHasFull());
    assertTrue(status.othersMayUpdate());
    assertTrue(status.anyCanRead());

    ACL a3 = new ACL(ZooDefs.Perms.READ, new Id("digest", "reader"));
    status = ZooAclUtil.checkWritableAuth(List.of(a3));
    assertFalse(status.accumuloHasFull());
    assertFalse(status.othersMayUpdate());
    assertTrue(status.anyCanRead());

    status = ZooAclUtil.checkWritableAuth(List.of(a1, a2, a3));
    assertTrue(status.accumuloHasFull());
    assertTrue(status.othersMayUpdate());
    assertTrue(status.anyCanRead());
  }
}
