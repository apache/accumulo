/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.server.conf.util;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.server.conf.store.PropCacheKey;
import org.apache.accumulo.server.conf.store.PropStoreException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides a lock used in property conversion. The lock is limit the number of processes that try
 * to create a property node and transform the legacy property format to the 2.1 encoded properties.
 * Processes do not queue for a lock (using a sequential node)
 * <p>
 * Features
 * <ul>
 * <li>Uses ephemeral node that will be removed if transform process terminates without
 * completing</li>
 * <li>Watcher not necessary - the existence of the node and uuid in data sufficient to detect
 * changes.</li>
 * </ul>
 */
public class TransformLock {
  public static final String LOCK_NAME = "/transform_lock";
  private static final Logger log = LoggerFactory.getLogger(TransformLock.class);
  private final LockId lockId = new LockId();
  private final String path;
  private final ZooReaderWriter zrw;
  private boolean locked = false;

  private TransformLock(final @NonNull PropCacheKey key, final ZooReaderWriter zrw) {
    path = key.getBasePath() + LOCK_NAME;
    this.zrw = zrw;
  }

  /**
   * Create a lock node in ZooKeeper using an ephemeral node. Will not throw and exception except on
   * an interrupt. If the lock node is created, the returned lock will be locked. If another lock
   * already exists, the lock is unlocked and the caller can decide to either wait for the resource
   * to be created by the thread that created the lock, or try calling to {@code lock} to succeed
   *
   * @param key
   *          a PropCacheKey that defines the storage location of the created lock and the
   *          associated property nodes.
   * @param zrw
   *          a ZooReaderWriter
   * @return an TransformLock instance.
   * @throws PropStoreException
   *           is the lock creation fails due to an underlying ZooKeeper exception.
   */
  public static TransformLock createLock(final @NonNull PropCacheKey key,
      final ZooReaderWriter zrw) {
    TransformLock lock = new TransformLock(key, zrw);
    lock.locked = lock.lock();
    return lock;
  }

  public boolean lock() {
    if (locked) {
      return true;
    }
    try {
      // existence check should be lighter-weight than failing on NODE_EXISTS exception
      if (zrw.exists(path)) {
        return false;
      }
      // if this completes this thread has created the lock
      zrw.putEphemeralData(path, lockId.asBytes());
      log.trace("wrote property upgrade lock: {} - {}", path, lockId);
      return true;
    } catch (KeeperException ex) {
      log.debug(
          "Failed to write transform lock for " + path + " another process may have created one",
          ex);
      return false;
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new PropStoreException("Interrupted getting transform lock", ex);
    }
  }

  /**
   * Return the lock status
   *
   * @return true if this instance has created the lock, false otherwise.
   */
  public boolean isLocked() {
    return locked;
  }

  /**
   * Verify lock is still present and valid while keeping the lock.
   *
   * @return true if lock is valid, false otherwise
   */
  public boolean validateLock() {
    try {
      byte[] readId = zrw.getData(path);
      log.trace("validate lock: read: {} - expected: {}", readId, lockId);
      return Arrays.equals(readId, lockId.asBytes());
    } catch (KeeperException ex) {
      throw new PropStoreException("Failed to validate lock", ex);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new PropStoreException("Interrupted while validating lock", ex);
    }
  }

  /**
   * If the lock was created by this instance the uuid created nad the uuid stored in the ZooKeeper
   * data will match.
   */
  public void unLock() {
    try {
      log.trace("unlock called - {} - exists in ZooKeeper: {}", path, zrw.exists(path));

      Stat stat = new Stat();
      byte[] readId = zrw.getData(path, stat);
      if (!Arrays.equals(readId, lockId.asBytes())) {
        throw new PropStoreException("tried to unlock a lock that was not held by current thread",
            null);
      }

      log.trace("unlock read id: {} - exists: {}", readId, zrw.exists(path));

      // make sure we are deleting the same node version just checked.
      zrw.deleteStrict(path, stat.getVersion());
    } catch (KeeperException ex) {
      throw new PropStoreException("Failed to unlock transform lock for " + path, ex);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new PropStoreException("Interrupted getting transform lock", ex);
    }
    locked = false;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    TransformLock that = (TransformLock) o;
    return path.equals(that.path) && Arrays.equals(lockId.asBytes(), that.lockId.asBytes());
  }

  @Override
  public int hashCode() {
    return Objects.hash(path, Arrays.hashCode(lockId.asBytes()));
  }

  @Override
  public String toString() {
    return "TransformLock{ path='" + path + "', locked='" + locked + "' id=" + lockId + "'}'";
  }

  private static class LockId {
    private final String id = UUID.randomUUID().toString();
    private final byte[] idBytes = id.getBytes(UTF_8);

    public byte[] asBytes() {
      return idBytes;
    }

    @Override
    public String toString() {
      return "LockId{id='" + id + '}';
    }
  }
}
