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
package org.apache.accumulo.server.conf.util;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides a token used in property conversion. The token is used to limit the number of processes
 * that try to create a property node and transform the legacy property format to the 2.1 encoded
 * properties. Processes do not queue for a token (using a sequential node) - processes should look
 * for the token to exist, and if present wait and then periodically re-check for the property node
 * to be created by the process that created / has the token.
 * <p>
 * Features
 * <ul>
 * <li>Uses ephemeral node that will be removed if transform process terminates without
 * completing</li>
 * <li>Watcher not necessary - the existence of the node and uuid in data sufficient to detect
 * changes.</li>
 * </ul>
 */
public class TransformToken {
  public static final String TRANSFORM_TOKEN = "/transform_token";
  private static final Logger log = LoggerFactory.getLogger(TransformToken.class);
  private final TokenUUID tokenUUID = new TokenUUID();
  private final String path;
  private final ZooReaderWriter zrw;
  private boolean haveToken = false;

  private TransformToken(final @NonNull String basePath, final ZooReaderWriter zrw) {
    path = basePath + TRANSFORM_TOKEN;
    this.zrw = zrw;

    boolean t = getTokenOwnership();
    log.trace("created token - token held: {}", t);
  }

  /**
   * Create a lock node in ZooKeeper using an ephemeral node. Will not throw and exception except on
   * an interrupt. If the lock node is created, the returned lock will be locked. If another lock
   * already exists, the lock is unlocked and the caller can decide to either wait for the resource
   * to be created by the thread that created the lock, or try calling to {@code lock} to succeed
   *
   * @param path the parent node of the legacy properties and the associated property children
   *        nodes.
   * @param zrw a ZooReaderWriter
   * @return an TransformLock instance.
   * @throws IllegalStateException is the lock creation fails due to an underlying ZooKeeper
   *         exception.
   */
  public static TransformToken createToken(final @NonNull String path, final ZooReaderWriter zrw) {
    return new TransformToken(path, zrw);
  }

  /**
   * Create and try to establish ownership (hold the token). Token ownership can be tested with
   * {@link #haveTokenOwnership() haveTokenOwnership}
   *
   * @return true if able to get ownership, false otherwise.
   */
  public boolean getTokenOwnership() {
    if (haveToken) {
      return true;
    }
    try {
      // existence check should be lighter-weight than failing on NODE_EXISTS exception
      if (zrw.exists(path)) {
        return false;
      }
      // if this completes this thread has created the lock
      zrw.putEphemeralData(path, tokenUUID.asBytes());
      log.trace("wrote property transform token: {} - {}", path, tokenUUID);
      haveToken = true;
      return true;
    } catch (KeeperException ex) {
      log.debug(
          "Failed to write transform token for " + path + " another process may have created one",
          ex);
      return false;
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted getting transform token", ex);
    }
  }

  /**
   * Return the token ownership status
   *
   * @return true if this instance has ownership of the token, false otherwise.
   */
  public boolean haveTokenOwnership() {
    return haveToken;
  }

  /**
   * Verify ownership is still valid while holding the token.
   *
   * @return true if token is still owned, false otherwise
   */
  public boolean validateToken() {
    try {
      byte[] readId = zrw.getData(path);
      log.trace("validate token: read: {} - expected: {}", readId, tokenUUID);
      return Arrays.equals(readId, tokenUUID.asBytes());
    } catch (KeeperException ex) {
      throw new IllegalStateException("Failed to validate token", ex);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while validating token", ex);
    }
  }

  /**
   * If the token was created by this instance, the uuid of this instance and the uuid stored in the
   * ZooKeeper data will match.
   */
  public void releaseToken() {
    try {
      if (log.isTraceEnabled()) {
        log.trace("releaseToken called - {} - exists in ZooKeeper: {}", path, zrw.exists(path));
      }

      Stat stat = new Stat();
      byte[] readId = zrw.getData(path, stat);
      if (!Arrays.equals(readId, tokenUUID.asBytes())) {
        throw new IllegalStateException(
            "tried to release a token that was not held by current thread");
      }

      if (log.isTraceEnabled()) {
        log.trace("releaseToken read id: {} - exists: {}", readId, zrw.exists(path));
      }

      // make sure we are deleting the same node version just checked.
      zrw.deleteStrict(path, stat.getVersion());
    } catch (KeeperException ex) {
      throw new IllegalStateException("Failed to release transform lock for " + path, ex);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted getting transform token", ex);
    }
    haveToken = false;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TransformToken that = (TransformToken) o;
    return path.equals(that.path) && Arrays.equals(tokenUUID.asBytes(), that.tokenUUID.asBytes());
  }

  @Override
  public int hashCode() {
    return Objects.hash(path, Arrays.hashCode(tokenUUID.asBytes()));
  }

  @Override
  public String toString() {
    return "TransformLock{ path='" + path + "', locked='" + haveToken + "' id=" + tokenUUID + "'}'";
  }

  private static class TokenUUID {
    private final String id = UUID.randomUUID().toString();
    private final byte[] idBytes = id.getBytes(UTF_8);

    public byte[] asBytes() {
      return idBytes;
    }

    @Override
    public String toString() {
      return "TransformToken{id='" + id + '}';
    }
  }
}
