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
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.zookeeper.KeeperException.AuthFailedException;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
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

  private ZooReaderWriter zrw;
  private String baseNode = Constants.ZDELEGATION_TOKEN_KEYS;

  @BeforeEach
  public void setupMocks() {
    zrw = createMock(ZooReaderWriter.class);
  }

  @Test
  public void testInitialize() throws Exception {
    ZooAuthenticationKeyDistributor distributor =
        new ZooAuthenticationKeyDistributor(zrw, baseNode);

    // Attempt to create the directory and fail
    expect(zrw.exists(baseNode)).andReturn(false);
    expect(
        zrw.putPrivatePersistentData(eq(baseNode), aryEq(new byte[0]), eq(NodeExistsPolicy.FAIL)))
        .andThrow(new AuthFailedException());

    replay(zrw);

    assertThrows(AuthFailedException.class, distributor::initialize);

    verify(zrw);
  }

  @Test
  public void testInitializeCreatesParentNode() throws Exception {
    ZooAuthenticationKeyDistributor distributor =
        new ZooAuthenticationKeyDistributor(zrw, baseNode);

    // Attempt to create the directory and fail
    expect(zrw.exists(baseNode)).andReturn(false);
    expect(zrw.putPrivatePersistentData(eq(baseNode), anyObject(), eq(NodeExistsPolicy.FAIL)))
        .andReturn(true);

    replay(zrw);

    distributor.initialize();

    verify(zrw);
  }

  @Test
  public void testInitializedNotCalledAdvertise() {
    ZooAuthenticationKeyDistributor distributor =
        new ZooAuthenticationKeyDistributor(zrw, baseNode);
    assertThrows(IllegalStateException.class,
        () -> distributor.advertise(new AuthenticationKey(1, 0L, 5L, keyGen.generateKey())));
  }

  @Test
  public void testInitializedNotCalledCurrentKeys() {
    ZooAuthenticationKeyDistributor distributor =
        new ZooAuthenticationKeyDistributor(zrw, baseNode);
    assertThrows(IllegalStateException.class, distributor::getCurrentKeys);
  }

  @Test
  public void testInitializedNotCalledRemove() {
    ZooAuthenticationKeyDistributor distributor =
        new ZooAuthenticationKeyDistributor(zrw, baseNode);
    assertThrows(IllegalStateException.class,
        () -> distributor.remove(new AuthenticationKey(1, 0L, 5L, keyGen.generateKey())));
  }

  @Test
  public void testMissingAcl() throws Exception {
    ZooAuthenticationKeyDistributor distributor =
        new ZooAuthenticationKeyDistributor(zrw, baseNode);
    // Attempt to create the directory and fail
    expect(zrw.exists(baseNode)).andReturn(true);
    expect(zrw.getACL(eq(baseNode))).andReturn(Collections.emptyList());

    replay(zrw);
    assertThrows(IllegalStateException.class, distributor::initialize);
    verify(zrw);
  }

  @Test
  public void testBadAcl() throws Exception {
    ZooAuthenticationKeyDistributor distributor =
        new ZooAuthenticationKeyDistributor(zrw, baseNode);
    // Attempt to create the directory and fail
    expect(zrw.exists(baseNode)).andReturn(true);
    expect(zrw.getACL(eq(baseNode))).andReturn(Collections.singletonList(
        new ACL(ZooUtil.PRIVATE.get(0).getPerms(), new Id("digest", "somethingweird"))));

    replay(zrw);
    assertThrows(IllegalStateException.class, distributor::initialize);
    verify(zrw);
  }

  @Test
  public void testAdvertiseKey() throws Exception {
    ZooAuthenticationKeyDistributor distributor =
        new ZooAuthenticationKeyDistributor(zrw, baseNode);
    AuthenticationKey key = new AuthenticationKey(1, 0L, 10L, keyGen.generateKey());
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    key.write(new DataOutputStream(baos));
    byte[] serialized = baos.toByteArray();
    String path = baseNode + "/" + key.getKeyId();

    // Attempt to create the directory and fail
    expect(zrw.exists(baseNode)).andReturn(true);
    expect(zrw.getACL(eq(baseNode))).andReturn(Collections.singletonList(
        new ACL(ZooUtil.PRIVATE.get(0).getPerms(), new Id("digest", "accumulo:DEFAULT"))));
    expect(zrw.exists(path)).andReturn(false);
    expect(zrw.putPrivatePersistentData(eq(path), aryEq(serialized), eq(NodeExistsPolicy.FAIL)))
        .andReturn(true);

    replay(zrw);

    distributor.initialize();
    distributor.advertise(key);

    verify(zrw);
  }

  @Test
  public void testAlreadyAdvertisedKey() throws Exception {
    ZooAuthenticationKeyDistributor distributor =
        new ZooAuthenticationKeyDistributor(zrw, baseNode);
    AuthenticationKey key = new AuthenticationKey(1, 0L, 10L, keyGen.generateKey());
    String path = baseNode + "/" + key.getKeyId();

    // Attempt to create the directory and fail
    expect(zrw.exists(baseNode)).andReturn(true);
    expect(zrw.getACL(eq(baseNode))).andReturn(Collections.singletonList(
        new ACL(ZooUtil.PRIVATE.get(0).getPerms(), new Id("digest", "accumulo:DEFAULT"))));
    expect(zrw.exists(path)).andReturn(true);

    replay(zrw);

    distributor.initialize();
    distributor.advertise(key);

    verify(zrw);
  }

  @Test
  public void testRemoveKey() throws Exception {
    ZooAuthenticationKeyDistributor distributor =
        new ZooAuthenticationKeyDistributor(zrw, baseNode);
    AuthenticationKey key = new AuthenticationKey(1, 0L, 10L, keyGen.generateKey());
    String path = baseNode + "/" + key.getKeyId();

    // Attempt to create the directory and fail
    expect(zrw.exists(baseNode)).andReturn(true);
    expect(zrw.getACL(eq(baseNode))).andReturn(Collections.singletonList(
        new ACL(ZooUtil.PRIVATE.get(0).getPerms(), new Id("digest", "accumulo:DEFAULT"))));
    expect(zrw.exists(path)).andReturn(true);
    zrw.delete(path);
    expectLastCall().once();

    replay(zrw);

    distributor.initialize();
    distributor.remove(key);

    verify(zrw);
  }

  @Test
  public void testRemoveMissingKey() throws Exception {
    ZooAuthenticationKeyDistributor distributor =
        new ZooAuthenticationKeyDistributor(zrw, baseNode);
    AuthenticationKey key = new AuthenticationKey(1, 0L, 10L, keyGen.generateKey());
    String path = baseNode + "/" + key.getKeyId();

    // Attempt to create the directory and fail
    expect(zrw.exists(baseNode)).andReturn(true);
    expect(zrw.getACL(eq(baseNode))).andReturn(Collections.singletonList(
        new ACL(ZooUtil.PRIVATE.get(0).getPerms(), new Id("digest", "accumulo:DEFAULT"))));
    expect(zrw.exists(path)).andReturn(false);

    replay(zrw);

    distributor.initialize();
    distributor.remove(key);

    verify(zrw);
  }

  @Test
  public void testGetCurrentKeys() throws Exception {
    ZooAuthenticationKeyDistributor distributor =
        new ZooAuthenticationKeyDistributor(zrw, baseNode);
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

    expect(zrw.exists(baseNode)).andReturn(true);
    expect(zrw.getACL(eq(baseNode))).andReturn(Collections.singletonList(
        new ACL(ZooUtil.PRIVATE.get(0).getPerms(), new Id("digest", "accumulo:DEFAULT"))));
    expect(zrw.getChildren(baseNode)).andReturn(children);
    for (int i = 1; i < 6; i++) {
      expect(zrw.getData(baseNode + "/" + i)).andReturn(serializedKeys.get(i - 1));
    }

    replay(zrw);

    distributor.initialize();
    assertEquals(keys, distributor.getCurrentKeys());

    verify(zrw);
  }
}
