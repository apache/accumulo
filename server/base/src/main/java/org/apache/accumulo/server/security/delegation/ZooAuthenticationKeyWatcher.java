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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import org.apache.accumulo.fate.zookeeper.ZooReader;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Watch ZooKeeper to notice changes in the published keys so that authenticate can properly occur using delegation tokens.
 */
public class ZooAuthenticationKeyWatcher implements Watcher {
  private static final Logger log = LoggerFactory.getLogger(ZooAuthenticationKeyWatcher.class);

  private final AuthenticationTokenSecretManager secretManager;
  private final ZooReader zk;
  private final String baseNode;

  public ZooAuthenticationKeyWatcher(AuthenticationTokenSecretManager secretManager, ZooReader zk, String baseNode) {
    this.secretManager = secretManager;
    this.zk = zk;
    this.baseNode = baseNode;
  }

  @Override
  public void process(WatchedEvent event) {
    if (EventType.None == event.getType()) {
      switch (event.getState()) {
        case Disconnected: // Intentional fall through of case
        case Expired: // ZooReader is handling the Expiration of the original ZooKeeper object for us
          log.debug("ZooKeeper connection disconnected, clearing secret manager");
          secretManager.removeAllKeys();
          break;
        case SyncConnected:
          log.debug("ZooKeeper reconnected, updating secret manager");
          try {
            updateAuthKeys();
          } catch (KeeperException | InterruptedException e) {
            log.error("Failed to update secret manager after ZooKeeper reconnect");
          }
          break;
        default:
          log.warn("Unhandled: {}", event);
      }

      // Nothing more to do for EventType.None
      return;
    }

    String path = event.getPath();
    if (null == path) {
      return;
    }

    if (!path.startsWith(baseNode)) {
      log.info("Ignoring event for path: {}", path);
      return;
    }

    try {
      if (path.equals(baseNode)) {
        processBaseNode(event);
      } else {
        processChildNode(event);
      }
    } catch (KeeperException | InterruptedException e) {
      log.error("Failed to communicate with ZooKeeper", e);
    }
  }

  /**
   * Process the {@link WatchedEvent} for the base znode that the {@link AuthenticationKey}s are stored in.
   */
  void processBaseNode(WatchedEvent event) throws KeeperException, InterruptedException {
    switch (event.getType()) {
      case NodeDeleted:
        // The parent node was deleted, no children are possible, remove all keys
        log.debug("Parent ZNode was deleted, removing all AuthenticationKeys");
        secretManager.removeAllKeys();
        break;
      case None:
        // Not connected, don't care
        break;
      case NodeCreated: // intentional fall-through to NodeChildrenChanged
      case NodeChildrenChanged:
        // Process each child, and reset the watcher on the parent node. We know that the node exists
        updateAuthKeys(event.getPath());
        break;
      case NodeDataChanged:
        // The data on the parent changed. We aren't storing anything there so it's a noop
        break;
      default:
        log.warn("Unsupported event type: {}", event.getType());
        break;
    }
  }

  /**
   * Entry point to seed the local {@link AuthenticationKey} cache from ZooKeeper and set the first watcher for future updates in ZooKeeper.
   */
  public void updateAuthKeys() throws KeeperException, InterruptedException {
    // Might cause two watchers on baseNode, but only at startup for each tserver.
    if (zk.exists(baseNode, this)) {
      log.info("Added {} existing AuthenticationKeys to local cache from ZooKeeper", updateAuthKeys(baseNode));
    }
  }

  private int updateAuthKeys(String path) throws KeeperException, InterruptedException {
    int keysAdded = 0;
    for (String child : zk.getChildren(path, this)) {
      String childPath = path + "/" + child;
      try {
        // Get the node data and reset the watcher
        AuthenticationKey key = deserializeKey(zk.getData(childPath, this, null));
        secretManager.addKey(key);
        keysAdded++;
      } catch (NoNodeException e) {
        // The master expired(deleted) the key between when we saw it in getChildren() and when we went to add it to our secret manager.
        log.trace("{} was deleted when we tried to access it", childPath);
      }
    }
    return keysAdded;
  }

  /**
   * Process the {@link WatchedEvent} for a node which represents an {@link AuthenticationKey}
   */
  void processChildNode(WatchedEvent event) throws KeeperException, InterruptedException {
    final String path = event.getPath();
    switch (event.getType()) {
      case NodeDeleted:
        // Key expired
        if (null == path) {
          log.error("Got null path for NodeDeleted event");
          return;
        }

        // Pull off the base ZK path and the '/' separator
        String childName = path.substring(baseNode.length() + 1);
        secretManager.removeKey(Integer.parseInt(childName));
        break;
      case None:
        // Not connected, don't care. We'll update when we're reconnected
        break;
      case NodeCreated:
        // New key created
        if (null == path) {
          log.error("Got null path for NodeCreated event");
          return;
        }
        // Get the data and reset the watcher
        AuthenticationKey key = deserializeKey(zk.getData(path, this, null));
        log.debug("Adding AuthenticationKey with keyId {}", key.getKeyId());
        secretManager.addKey(key);
        break;
      case NodeDataChanged:
        // Key changed, could happen on restart after not running Accumulo.
        if (null == path) {
          log.error("Got null path for NodeDataChanged event");
          return;
        }
        // Get the data and reset the watcher
        AuthenticationKey newKey = deserializeKey(zk.getData(path, this, null));
        // Will overwrite the old key if one exists
        secretManager.addKey(newKey);
        break;
      case NodeChildrenChanged:
        // no children for the children..
        log.warn("Unexpected NodeChildrenChanged event for authentication key node {}", path);
        break;
      default:
        log.warn("Unsupported event type: {}", event.getType());
        break;
    }
  }

  /**
   * Deserialize the bytes into an {@link AuthenticationKey}
   */
  AuthenticationKey deserializeKey(byte[] serializedKey) {
    AuthenticationKey key = new AuthenticationKey();
    try {
      key.readFields(new DataInputStream(new ByteArrayInputStream(serializedKey)));
    } catch (IOException e) {
      throw new AssertionError("Failed to read from an in-memory buffer");
    }
    return key;
  }
}
