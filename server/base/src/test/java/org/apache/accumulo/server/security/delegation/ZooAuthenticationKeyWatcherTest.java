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

import static java.util.concurrent.TimeUnit.DAYS;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import javax.crypto.KeyGenerator;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.fate.zookeeper.ZooReader;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ZooAuthenticationKeyWatcherTest {

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

  private ZooReader zk;
  private InstanceId instanceId;
  private String baseNode;
  private long tokenLifetime = DAYS.toMillis(7);
  private AuthenticationTokenSecretManager secretManager;
  private ZooAuthenticationKeyWatcher keyWatcher;

  @BeforeEach
  public void setupMocks() {
    zk = createMock(ZooReader.class);
    instanceId = InstanceId.of(UUID.randomUUID());
    baseNode = "/accumulo/" + instanceId + Constants.ZDELEGATION_TOKEN_KEYS;
    secretManager = new AuthenticationTokenSecretManager(instanceId, tokenLifetime);
    keyWatcher = new ZooAuthenticationKeyWatcher(secretManager, zk, baseNode);
  }

  @Test
  public void testBaseNodeCreated() throws Exception {
    WatchedEvent event = new WatchedEvent(EventType.NodeCreated, null, baseNode);

    expect(zk.getChildren(baseNode, keyWatcher)).andReturn(Collections.emptyList());
    replay(zk);

    keyWatcher.process(event);

    verify(zk);
    assertTrue(secretManager.getKeys().isEmpty());
  }

  @Test
  public void testBaseNodeCreatedWithChildren() throws Exception {
    WatchedEvent event = new WatchedEvent(EventType.NodeCreated, null, baseNode);
    AuthenticationKey key1 = new AuthenticationKey(1, 0L, 10000L, keyGen.generateKey()),
        key2 = new AuthenticationKey(2, key1.getExpirationDate(), 20000L, keyGen.generateKey());
    byte[] serializedKey1 = serialize(key1), serializedKey2 = serialize(key2);
    List<String> children = Arrays.asList("1", "2");

    expect(zk.getChildren(baseNode, keyWatcher)).andReturn(children);
    expect(zk.getData(baseNode + "/1", keyWatcher)).andReturn(serializedKey1);
    expect(zk.getData(baseNode + "/2", keyWatcher)).andReturn(serializedKey2);
    replay(zk);

    keyWatcher.process(event);

    verify(zk);
    assertEquals(2, secretManager.getKeys().size());
    assertEquals(key1, secretManager.getKeys().get(key1.getKeyId()));
    assertEquals(key2, secretManager.getKeys().get(key2.getKeyId()));
  }

  @Test
  public void testBaseNodeChildrenChanged() throws Exception {
    WatchedEvent event = new WatchedEvent(EventType.NodeChildrenChanged, null, baseNode);
    AuthenticationKey key1 = new AuthenticationKey(1, 0L, 10000L, keyGen.generateKey()),
        key2 = new AuthenticationKey(2, key1.getExpirationDate(), 20000L, keyGen.generateKey());
    byte[] serializedKey1 = serialize(key1), serializedKey2 = serialize(key2);
    List<String> children = Arrays.asList("1", "2");

    expect(zk.getChildren(baseNode, keyWatcher)).andReturn(children);
    expect(zk.getData(baseNode + "/1", keyWatcher)).andReturn(serializedKey1);
    expect(zk.getData(baseNode + "/2", keyWatcher)).andReturn(serializedKey2);
    replay(zk);

    keyWatcher.process(event);

    verify(zk);
    assertEquals(2, secretManager.getKeys().size());
    assertEquals(key1, secretManager.getKeys().get(key1.getKeyId()));
    assertEquals(key2, secretManager.getKeys().get(key2.getKeyId()));
  }

  @Test
  public void testBaseNodeDeleted() {
    WatchedEvent event = new WatchedEvent(EventType.NodeDeleted, null, baseNode);
    AuthenticationKey key1 = new AuthenticationKey(1, 0L, 10000L, keyGen.generateKey()),
        key2 = new AuthenticationKey(2, key1.getExpirationDate(), 20000L, keyGen.generateKey());

    secretManager.addKey(key1);
    secretManager.addKey(key2);
    assertEquals(2, secretManager.getKeys().size());

    replay(zk);

    keyWatcher.process(event);

    verify(zk);
    assertEquals(0, secretManager.getKeys().size());
    assertFalse(secretManager.isCurrentKeySet());
  }

  @Test
  public void testBaseNodeDataChanged() {
    WatchedEvent event = new WatchedEvent(EventType.NodeDataChanged, null, baseNode);

    replay(zk);

    keyWatcher.process(event);

    verify(zk);
    assertEquals(0, secretManager.getKeys().size());
    assertFalse(secretManager.isCurrentKeySet());
  }

  @Test
  public void testChildChanged() throws Exception {
    WatchedEvent event = new WatchedEvent(EventType.NodeCreated, null, baseNode + "/2");
    AuthenticationKey key1 = new AuthenticationKey(1, 0L, 10000L, keyGen.generateKey()),
        key2 = new AuthenticationKey(2, key1.getExpirationDate(), 20000L, keyGen.generateKey());
    secretManager.addKey(key1);
    assertEquals(1, secretManager.getKeys().size());
    byte[] serializedKey2 = serialize(key2);

    expect(zk.getData(event.getPath(), keyWatcher)).andReturn(serializedKey2);
    replay(zk);

    keyWatcher.process(event);

    verify(zk);
    assertEquals(2, secretManager.getKeys().size());
    assertEquals(key1, secretManager.getKeys().get(key1.getKeyId()));
    assertEquals(key2, secretManager.getKeys().get(key2.getKeyId()));
    assertEquals(key2, secretManager.getCurrentKey());
  }

  @Test
  public void testChildDeleted() {
    WatchedEvent event = new WatchedEvent(EventType.NodeDeleted, null, baseNode + "/1");
    AuthenticationKey key1 = new AuthenticationKey(1, 0L, 10000L, keyGen.generateKey()),
        key2 = new AuthenticationKey(2, key1.getExpirationDate(), 20000L, keyGen.generateKey());
    secretManager.addKey(key1);
    secretManager.addKey(key2);
    assertEquals(2, secretManager.getKeys().size());

    replay(zk);

    keyWatcher.process(event);

    verify(zk);
    assertEquals(1, secretManager.getKeys().size());
    assertEquals(key2, secretManager.getKeys().get(key2.getKeyId()));
    assertEquals(key2, secretManager.getCurrentKey());
  }

  @Test
  public void testChildChildrenChanged() {
    WatchedEvent event = new WatchedEvent(EventType.NodeChildrenChanged, null, baseNode + "/2");
    AuthenticationKey key1 = new AuthenticationKey(1, 0L, 10000L, keyGen.generateKey()),
        key2 = new AuthenticationKey(2, key1.getExpirationDate(), 20000L, keyGen.generateKey());
    secretManager.addKey(key1);
    secretManager.addKey(key2);
    assertEquals(2, secretManager.getKeys().size());

    replay(zk);

    // Does nothing
    keyWatcher.process(event);

    verify(zk);
    assertEquals(2, secretManager.getKeys().size());
    assertEquals(key1, secretManager.getKeys().get(key1.getKeyId()));
    assertEquals(key2, secretManager.getKeys().get(key2.getKeyId()));
    assertEquals(key2, secretManager.getCurrentKey());
  }

  @Test
  public void testInitialUpdateNoNode() throws Exception {
    expect(zk.exists(baseNode, keyWatcher)).andReturn(false);

    replay(zk);

    keyWatcher.updateAuthKeys();

    verify(zk);
    assertEquals(0, secretManager.getKeys().size());
    assertNull(secretManager.getCurrentKey());
  }

  @Test
  public void testInitialUpdateWithKeys() throws Exception {
    List<String> children = Arrays.asList("1", "5");
    AuthenticationKey key1 = new AuthenticationKey(1, 0L, 10000L, keyGen.generateKey()),
        key2 = new AuthenticationKey(5, key1.getExpirationDate(), 20000L, keyGen.generateKey());

    expect(zk.exists(baseNode, keyWatcher)).andReturn(true);
    expect(zk.getChildren(baseNode, keyWatcher)).andReturn(children);
    expect(zk.getData(baseNode + "/" + key1.getKeyId(), keyWatcher)).andReturn(serialize(key1));
    expect(zk.getData(baseNode + "/" + key2.getKeyId(), keyWatcher)).andReturn(serialize(key2));

    replay(zk);

    keyWatcher.updateAuthKeys();

    verify(zk);

    assertEquals(2, secretManager.getKeys().size());
    assertEquals(key1, secretManager.getKeys().get(key1.getKeyId()));
    assertEquals(key2, secretManager.getKeys().get(key2.getKeyId()));
  }

  @Test
  public void testDisconnectAndReconnect() throws Exception {
    lostZooKeeperBase(new WatchedEvent(EventType.None, KeeperState.Disconnected, null),
        new WatchedEvent(EventType.None, KeeperState.SyncConnected, null));
  }

  @Test
  public void testExpiredAndReconnect() throws Exception {
    lostZooKeeperBase(new WatchedEvent(EventType.None, KeeperState.Expired, null),
        new WatchedEvent(EventType.None, KeeperState.SyncConnected, null));
  }

  private void lostZooKeeperBase(WatchedEvent disconnectEvent, WatchedEvent reconnectEvent)
      throws Exception {

    List<String> children = Arrays.asList("1", "5");
    AuthenticationKey key1 = new AuthenticationKey(1, 0L, 10000L, keyGen.generateKey()),
        key2 = new AuthenticationKey(5, key1.getExpirationDate(), 20000L, keyGen.generateKey());

    expect(zk.exists(baseNode, keyWatcher)).andReturn(true);
    expect(zk.getChildren(baseNode, keyWatcher)).andReturn(children);
    expect(zk.getData(baseNode + "/" + key1.getKeyId(), keyWatcher)).andReturn(serialize(key1));
    expect(zk.getData(baseNode + "/" + key2.getKeyId(), keyWatcher)).andReturn(serialize(key2));

    replay(zk);

    // Initialize and then get disconnected
    keyWatcher.updateAuthKeys();
    keyWatcher.process(disconnectEvent);

    verify(zk);

    // We should have no auth keys when we're disconnected
    assertEquals(0, secretManager.getKeys().size(),
        "Secret manager should be empty after a disconnect");
    assertNull(secretManager.getCurrentKey(), "Current key should be null");

    reset(zk);

    expect(zk.exists(baseNode, keyWatcher)).andReturn(true);
    expect(zk.getChildren(baseNode, keyWatcher)).andReturn(children);
    expect(zk.getData(baseNode + "/" + key1.getKeyId(), keyWatcher)).andReturn(serialize(key1));
    expect(zk.getData(baseNode + "/" + key2.getKeyId(), keyWatcher)).andReturn(serialize(key2));

    replay(zk);

    // Reconnect again, get all the keys
    keyWatcher.process(reconnectEvent);

    verify(zk);

    // Verify we have both keys
    assertEquals(2, secretManager.getKeys().size());
    assertEquals(key1, secretManager.getKeys().get(key1.getKeyId()));
    assertEquals(key2, secretManager.getKeys().get(key2.getKeyId()));
  }

  @Test
  public void missingKeyAfterGetChildren() throws Exception {
    List<String> children = Arrays.asList("1");
    AuthenticationKey key1 = new AuthenticationKey(1, 0L, 10000L, keyGen.generateKey());

    expect(zk.exists(baseNode, keyWatcher)).andReturn(true);
    // We saw key1
    expect(zk.getChildren(baseNode, keyWatcher)).andReturn(children);
    // but it was gone when we tried to access it (manager deleted it)
    expect(zk.getData(baseNode + "/" + key1.getKeyId(), keyWatcher))
        .andThrow(new NoNodeException());

    replay(zk);

    // Initialize
    keyWatcher.updateAuthKeys();

    verify(zk);

    // We should have no auth keys after initializing things
    assertEquals(0, secretManager.getKeys().size(),
        "Secret manager should be empty after a disconnect");
    assertNull(secretManager.getCurrentKey(), "Current key should be null");
  }

  private byte[] serialize(AuthenticationKey key) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    key.write(new DataOutputStream(baos));
    return baos.toByteArray();
  }
}
