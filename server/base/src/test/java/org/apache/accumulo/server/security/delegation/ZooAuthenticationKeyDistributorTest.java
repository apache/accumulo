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
package org.apache.accumulo.server.security.delegation;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.aryEq;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.isNull;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.crypto.KeyGenerator;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.AuthFailedException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ZooAuthenticationKeyDistributorTest {

  // From org.apache.hadoop.security.token.SecretManager
  private static final String DEFAULT_HMAC_ALGORITHM = "HmacSHA1";
  private static final int KEY_LENGTH = 64;
  private static KeyGenerator keyGen;

  @BeforeAll
  public static void setupKeyGenerator() throws Exception {
    // From org.apache.hadoop.security.token.SecretManager
    keyGen = KeyGenerator.getInstance(DEFAULT_HMAC_ALGORITHM);
    keyGen.init(KEY_LENGTH);
  }

  private ZooKeeper zk;
  private final String baseNode = Constants.ZDELEGATION_TOKEN_KEYS;
  private ZooAuthenticationKeyDistributor distributor;

  @BeforeEach
  public void setupMocks() {
    zk = createMock(ZooKeeper.class);
    distributor = new ZooAuthenticationKeyDistributor(zk, baseNode);
  }

  @AfterEach
  public void verifyMocks() {
    verify(zk);
  }

  @Test
  public void testInitialize() throws Exception {
    // Attempt to create the directory and fail
    expect(zk.exists(baseNode, null)).andReturn(null);
    expect(zk.create(eq(baseNode), aryEq(new byte[0]), anyObject(), eq(CreateMode.PERSISTENT)))
        .andThrow(new AuthFailedException());

    replay(zk);

    assertThrows(AuthFailedException.class, distributor::initialize);
  }

  @Test
  public void testInitializeCreatesParentNode() throws Exception {
    // Attempt to create the directory and fail
    expect(zk.exists(baseNode, null)).andReturn(null);
    expect(zk.create(eq(baseNode), anyObject(), anyObject(), eq(CreateMode.PERSISTENT)))
        .andReturn(baseNode);

    replay(zk);

    distributor.initialize();
  }

  @Test
  public void testInitializedNotCalledAdvertise() {
    replay(zk);
    assertThrows(IllegalStateException.class,
        () -> distributor.advertise(new AuthenticationKey(1, 0L, 5L, keyGen.generateKey())));
  }

  @Test
  public void testInitializedNotCalledCurrentKeys() {
    replay(zk);
    assertThrows(IllegalStateException.class, distributor::getCurrentKeys);
  }

  @Test
  public void testInitializedNotCalledRemove() {
    replay(zk);
    assertThrows(IllegalStateException.class,
        () -> distributor.remove(new AuthenticationKey(1, 0L, 5L, keyGen.generateKey())));
  }

  @Test
  public void testMissingAcl() throws Exception {
    // Attempt to create the directory and fail
    expect(zk.exists(baseNode, null)).andReturn(new Stat());
    expect(zk.getACL(eq(baseNode), isNull())).andReturn(Collections.emptyList());

    replay(zk);
    assertThrows(IllegalStateException.class, distributor::initialize);
  }

  @Test
  public void testBadAcl() throws Exception {
    // Attempt to create the directory and fail
    expect(zk.exists(baseNode, null)).andReturn(new Stat());
    expect(zk.getACL(eq(baseNode), isNull())).andReturn(Collections.singletonList(
        new ACL(ZooUtil.PRIVATE.get(0).getPerms(), new Id("digest", "somethingweird"))));

    replay(zk);
    assertThrows(IllegalStateException.class, distributor::initialize);
  }

  @Test
  public void testAdvertiseKey() throws Exception {
    AuthenticationKey key = new AuthenticationKey(1, 0L, 10L, keyGen.generateKey());
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    key.write(new DataOutputStream(baos));
    byte[] serialized = baos.toByteArray();
    String path = baseNode + "/" + key.getKeyId();

    // Attempt to create the directory and fail
    expect(zk.exists(baseNode, null)).andReturn(new Stat());
    expect(zk.getACL(eq(baseNode), isNull())).andReturn(Collections.singletonList(
        new ACL(ZooUtil.PRIVATE.get(0).getPerms(), new Id("digest", "accumulo:DEFAULT"))));
    expect(zk.exists(path, null)).andReturn(null);
    expect(zk.create(eq(path), aryEq(serialized), anyObject(), eq(CreateMode.PERSISTENT)))
        .andReturn(path);

    replay(zk);

    distributor.initialize();
    distributor.advertise(key);
  }

  @Test
  public void testAlreadyAdvertisedKey() throws Exception {
    AuthenticationKey key = new AuthenticationKey(1, 0L, 10L, keyGen.generateKey());
    String path = baseNode + "/" + key.getKeyId();

    // Attempt to create the directory and fail
    expect(zk.exists(baseNode, null)).andReturn(new Stat());
    expect(zk.getACL(eq(baseNode), isNull())).andReturn(Collections.singletonList(
        new ACL(ZooUtil.PRIVATE.get(0).getPerms(), new Id("digest", "accumulo:DEFAULT"))));
    expect(zk.exists(path, null)).andReturn(new Stat());

    replay(zk);

    distributor.initialize();
    distributor.advertise(key);
  }

  @Test
  public void testRemoveKey() throws Exception {
    AuthenticationKey key = new AuthenticationKey(1, 0L, 10L, keyGen.generateKey());
    String path = baseNode + "/" + key.getKeyId();

    // Attempt to create the directory and fail
    expect(zk.exists(baseNode, null)).andReturn(new Stat());
    expect(zk.getACL(eq(baseNode), isNull())).andReturn(Collections.singletonList(
        new ACL(ZooUtil.PRIVATE.get(0).getPerms(), new Id("digest", "accumulo:DEFAULT"))));
    expect(zk.exists(path, null)).andReturn(new Stat());
    zk.delete(path, -1);
    expectLastCall().once();

    replay(zk);

    distributor.initialize();
    distributor.remove(key);
  }

  @Test
  public void testRemoveMissingKey() throws Exception {
    AuthenticationKey key = new AuthenticationKey(1, 0L, 10L, keyGen.generateKey());
    String path = baseNode + "/" + key.getKeyId();

    // Attempt to create the directory and fail
    expect(zk.exists(baseNode, null)).andReturn(new Stat());
    expect(zk.getACL(eq(baseNode), isNull())).andReturn(Collections.singletonList(
        new ACL(ZooUtil.PRIVATE.get(0).getPerms(), new Id("digest", "accumulo:DEFAULT"))));
    expect(zk.exists(path, null)).andReturn(null);

    replay(zk);

    distributor.initialize();
    distributor.remove(key);
  }

  @Test
  public void testGetCurrentKeys() throws Exception {
    List<AuthenticationKey> keys = new ArrayList<>(5);
    List<byte[]> serializedKeys = new ArrayList<>(5);
    List<String> children = new ArrayList<>(5);
    for (int i = 1; i < 6; i++) {
      children.add(Integer.toString(i));
      AuthenticationKey key = new AuthenticationKey(i, 0L, 10L, keyGen.generateKey());
      keys.add(key);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      key.write(new DataOutputStream(baos));
      serializedKeys.add(baos.toByteArray());
    }

    expect(zk.exists(baseNode, null)).andReturn(new Stat());
    expect(zk.getACL(eq(baseNode), isNull())).andReturn(Collections.singletonList(
        new ACL(ZooUtil.PRIVATE.get(0).getPerms(), new Id("digest", "accumulo:DEFAULT"))));
    expect(zk.getChildren(baseNode, null)).andReturn(children);
    for (int i = 1; i < 6; i++) {
      expect(zk.getData(baseNode + "/" + i, null, null)).andReturn(serializedKeys.get(i - 1));
    }

    replay(zk);

    distributor.initialize();
    assertEquals(keys, distributor.getCurrentKeys());
  }
}
