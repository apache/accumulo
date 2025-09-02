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
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.accumulo.core.fate.zookeeper.ServiceLock.AccumuloLockWatcher;
import org.apache.accumulo.core.fate.zookeeper.ServiceLock.LockWatcher;
import org.apache.accumulo.core.fate.zookeeper.ServiceLock.ServiceLockPath;
import org.apache.accumulo.core.util.ServerServices;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Test;

public class ServiceLockTest {

  private final String ZPATH = "/test";
  private final ServiceLockPath path = ServiceLock.path(ZPATH);

  @Test
  public void testSortAndFindLowestPrevPrefix() {
    List<String> children = new ArrayList<>();
    children.add("zlock#00000000-0000-0000-0000-ffffffffffff#0000000007");
    children.add("zlock#00000000-0000-0000-0000-eeeeeeeeeeee#0000000010");
    children.add("zlock#00000000-0000-0000-0000-bbbbbbbbbbbb#0000000006");
    children.add("zlock#00000000-0000-0000-0000-dddddddddddd#0000000008");
    children.add("zlock#00000000-0000-0000-0000-bbbbbbbbbbbb#0000000004");
    children.add("zlock-123456789");
    children.add("zlock#00000000-0000-0000-0000-cccccccccccc#0000000003");
    children.add("zlock#00000000-0000-0000-0000-aaaaaaaaaaaa#0000000002");
    children.add("zlock#987654321");
    children.add("zlock#00000000-0000-0000-0000-aaaaaaaaaaaa#0000000001");

    final List<String> validChildren = ServiceLock.validateAndSort(ServiceLock.path(""), children);

    assertEquals(8, validChildren.size());
    assertEquals("zlock#00000000-0000-0000-0000-aaaaaaaaaaaa#0000000001", validChildren.get(0));
    assertEquals("zlock#00000000-0000-0000-0000-aaaaaaaaaaaa#0000000002", validChildren.get(1));
    assertEquals("zlock#00000000-0000-0000-0000-cccccccccccc#0000000003", validChildren.get(2));
    assertEquals("zlock#00000000-0000-0000-0000-bbbbbbbbbbbb#0000000004", validChildren.get(3));
    assertEquals("zlock#00000000-0000-0000-0000-bbbbbbbbbbbb#0000000006", validChildren.get(4));
    assertEquals("zlock#00000000-0000-0000-0000-ffffffffffff#0000000007", validChildren.get(5));
    assertEquals("zlock#00000000-0000-0000-0000-dddddddddddd#0000000008", validChildren.get(6));
    assertEquals("zlock#00000000-0000-0000-0000-eeeeeeeeeeee#0000000010", validChildren.get(7));

    assertEquals("zlock#00000000-0000-0000-0000-bbbbbbbbbbbb#0000000004",
        ServiceLock.findLowestPrevPrefix(validChildren,
            "zlock#00000000-0000-0000-0000-ffffffffffff#0000000007"));

    assertEquals("zlock#00000000-0000-0000-0000-aaaaaaaaaaaa#0000000001",
        ServiceLock.findLowestPrevPrefix(validChildren,
            "zlock#00000000-0000-0000-0000-cccccccccccc#0000000003"));

    assertEquals("zlock#00000000-0000-0000-0000-dddddddddddd#0000000008",
        ServiceLock.findLowestPrevPrefix(validChildren,
            "zlock#00000000-0000-0000-0000-eeeeeeeeeeee#0000000010"));

    assertThrows(IndexOutOfBoundsException.class,
        () -> ServiceLock.findLowestPrevPrefix(validChildren,
            "zlock#00000000-0000-0000-0000-aaaaaaaaaaaa#0000000001"));

    assertThrows(IndexOutOfBoundsException.class,
        () -> ServiceLock.findLowestPrevPrefix(validChildren,
            "zlock#00000000-0000-0000-0000-XXXXXXXXXXXX#0000000099"));
  }

  @Test
  public void rejectInvalidUUID() {
    List<String> children = new ArrayList<>();
    String uuid = "1-1-1-1-1";
    String seq = "1234567891";
    children.add("zlock#" + uuid + "#" + seq);

    // pass as UUID, but fail on string compare.
    assertEquals("00000001-0001-0001-0001-000000000001", UUID.fromString(uuid).toString());
    final List<String> validChildren = ServiceLock.validateAndSort(ServiceLock.path(""), children);
    assertEquals(0, validChildren.size());
  }

  @Test
  public void uuidTest() {
    List<String> children = new ArrayList<>();
    String uuid = "219ad0f6-ebe0-416e-a20f-c0f32922841d";
    String seq = "1234567891";
    children.add("zlock#" + uuid + "#" + seq);

    final List<String> validChildren = ServiceLock.validateAndSort(ServiceLock.path(""), children);
    assertEquals(1, validChildren.size());
    String candidate = validChildren.get(0);
    assertTrue(candidate.contains(uuid));
    assertTrue(candidate.contains(seq));
  }

  @Test
  public void determineLockOwnershipTest() throws Exception {
    final long ephemeralOwner = 123456789L;
    Stat existsStat = new Stat();
    existsStat.setEphemeralOwner(ephemeralOwner);

    ZooKeeper zk = createMock(ZooKeeper.class);
    expect(zk.exists(eq(ZPATH), anyObject(ServiceLock.class))).andReturn(existsStat);

    replay(zk);

    ServiceLock serviceLock = new ServiceLock(zk, path, UUID.randomUUID());
    AccumuloLockWatcher mockLockWatcher = EasyMock.createMock(AccumuloLockWatcher.class);

    Method determineLockOwnershipMethod = ServiceLock.class
        .getDeclaredMethod("determineLockOwnership", String.class, AccumuloLockWatcher.class);
    determineLockOwnershipMethod.setAccessible(true);

    String testEphemeralNode = "zlock#test-uuid#0000000001";

    InvocationTargetException exception = assertThrows(InvocationTargetException.class, () -> {
      determineLockOwnershipMethod.invoke(serviceLock, testEphemeralNode, mockLockWatcher);
    });

    assertEquals(exception.getTargetException().getClass(), IllegalStateException.class);
    assertTrue(exception.getTargetException().getMessage()
        .contains("Called determineLockOwnership() when ephemeralNodeName == null"));

    verify(zk);
  }

  @Test
  public void tryLockCleanupOnFailureTest() throws Exception {
    LockWatcher mockLockWatcher = EasyMock.createMock(LockWatcher.class);

    final long ephemeralOwner = 123456789L;
    final String zlock = "zlock#test-uuid#0000000001";
    Stat existsStat = new Stat();
    existsStat.setEphemeralOwner(ephemeralOwner);

    ZooKeeper zk = createMock(ZooKeeper.class);
    expect(zk.exists(eq(ZPATH), anyObject(ServiceLock.class))).andReturn(existsStat);
    expect(zk.getChildren(eq(ZPATH + "/" + zlock), anyObject(null))).andReturn(List.of());
    zk.delete(eq(ZPATH + "/" + zlock), eq(-1));
    expectLastCall();

    replay(zk);

    ServiceLock serviceLock = new ServiceLock(zk, path, UUID.randomUUID());

    Field createdNodeNameField = ServiceLock.class.getDeclaredField("createdNodeName");
    createdNodeNameField.setAccessible(true);
    createdNodeNameField.set(serviceLock, zlock);

    Field lockWasAcquiredField = ServiceLock.class.getDeclaredField("lockWasAcquired");
    lockWasAcquiredField.setAccessible(true);
    lockWasAcquiredField.set(serviceLock, false);

    serviceLock.tryLock(mockLockWatcher,
        new ServerServices("9443", ServerServices.Service.COMPACTOR_CLIENT).toString()
            .getBytes(UTF_8));

    createdNodeNameField.setAccessible(true);
    // Cast to string since field should be set to null
    String nullCreatedNodeName = (String) createdNodeNameField.get(serviceLock);
    assertNull(nullCreatedNodeName, "createdNodeName was not cleaned up after failed lock");
    verify(zk);
  }
}
