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

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import javax.crypto.SecretKey;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.admin.DelegationTokenConfig;
import org.apache.accumulo.core.clientImpl.AuthenticationTokenIdentifier;
import org.apache.accumulo.core.clientImpl.DelegationTokenImpl;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.securityImpl.thrift.TAuthenticationTokenIdentifier;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.Token;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Manages an internal list of secret keys used to sign new authentication tokens as they are
 * generated, and to validate existing tokens used for authentication.
 *
 * Each TabletServer, in addition to the Manager, has an instance of this {@link SecretManager} so
 * that each can authenticate requests from clients presenting delegation tokens. The Manager will
 * also run an instance of {@link AuthenticationTokenKeyManager} which handles generation of new
 * keys and removal of old keys. That class will call the methods here to ensure the in-memory cache
 * is consistent with what is advertised in ZooKeeper.
 */
public class AuthenticationTokenSecretManager extends SecretManager<AuthenticationTokenIdentifier> {

  private static final Logger log = LoggerFactory.getLogger(AuthenticationTokenSecretManager.class);

  private final InstanceId instanceID;
  private final long tokenMaxLifetime;
  private final ConcurrentHashMap<Integer,AuthenticationKey> allKeys = new ConcurrentHashMap<>();
  private AuthenticationKey currentKey;

  /**
   * Create a new secret manager instance for generating keys.
   *
   * @param instanceID Accumulo instance ID
   * @param tokenMaxLifetime Maximum age (in milliseconds) before a token expires and is no longer
   *        valid
   */
  public AuthenticationTokenSecretManager(InstanceId instanceID, long tokenMaxLifetime) {
    requireNonNull(instanceID);
    checkArgument(tokenMaxLifetime > 0, "Max lifetime must be positive");
    this.instanceID = instanceID;
    this.tokenMaxLifetime = tokenMaxLifetime;
  }

  private byte[] createPassword(AuthenticationTokenIdentifier identifier,
      DelegationTokenConfig cfg) {
    long now = System.currentTimeMillis();
    identifier.setIssueDate(now);
    identifier.setExpirationDate(calculateExpirationDate(now));
    // Limit the lifetime if the user requests it
    if (cfg != null) {
      long requestedLifetime = cfg.getTokenLifetime(TimeUnit.MILLISECONDS);
      if (requestedLifetime > 0) {
        long requestedExpirationDate = identifier.getIssueDate() + requestedLifetime;
        // Catch overflow again
        if (requestedExpirationDate < identifier.getIssueDate()) {
          requestedExpirationDate = Long.MAX_VALUE;
        }
        // Ensure that the user doesn't try to extend the expiration date -- they may only limit it
        if (requestedExpirationDate > identifier.getExpirationDate()) {
          throw new IllegalStateException("Requested token lifetime exceeds configured maximum");
        }
        log.trace("Overriding token expiration date from {} to {}", identifier.getExpirationDate(),
            requestedExpirationDate);
        identifier.setExpirationDate(requestedExpirationDate);
      }
    }
    return createPassword(identifier);
  }

  private long calculateExpirationDate(long now) {
    long expiration = now + tokenMaxLifetime;
    // Catch overflow
    if (expiration < now) {
      expiration = Long.MAX_VALUE;
    }
    return expiration;
  }

  @Override
  protected byte[] createPassword(AuthenticationTokenIdentifier identifier) {
    final AuthenticationKey secretKey;
    synchronized (this) {
      secretKey = currentKey;
    }
    identifier.setKeyId(secretKey.getKeyId());
    identifier.setInstanceId(instanceID);

    long now = System.currentTimeMillis();
    if (!identifier.isSetIssueDate()) {
      identifier.setIssueDate(now);
    }
    if (!identifier.isSetExpirationDate()) {
      identifier.setExpirationDate(calculateExpirationDate(now));
    }
    return createPassword(identifier.getBytes(), secretKey.getKey());
  }

  @Override
  public byte[] retrievePassword(AuthenticationTokenIdentifier identifier) throws InvalidToken {
    long now = System.currentTimeMillis();
    if (identifier.getExpirationDate() < now) {
      throw new InvalidToken("Token has expired");
    }
    if (identifier.getIssueDate() > now) {
      throw new InvalidToken("Token issued in the future");
    }
    AuthenticationKey managerKey = allKeys.get(identifier.getKeyId());
    if (managerKey == null) {
      throw new InvalidToken("Unknown manager key for token (id=" + identifier.getKeyId() + ")");
    }
    // regenerate the password
    return createPassword(identifier.getBytes(), managerKey.getKey());
  }

  @Override
  public AuthenticationTokenIdentifier createIdentifier() {
    // Return our TokenIdentifier implementation
    return new AuthenticationTokenIdentifier(new TAuthenticationTokenIdentifier());
  }

  /**
   * Generates a delegation token for the user with the provided {@code username}.
   *
   * @param username The client to generate the delegation token for.
   * @param cfg A configuration object for obtaining the delegation token
   * @return A delegation token for {@code username} created using the {@link #currentKey}.
   */
  public Entry<Token<AuthenticationTokenIdentifier>,AuthenticationTokenIdentifier>
      generateToken(String username, DelegationTokenConfig cfg) throws AccumuloException {
    requireNonNull(username);
    requireNonNull(cfg);

    var id = new AuthenticationTokenIdentifier(new TAuthenticationTokenIdentifier(username));

    final StringBuilder svcName = new StringBuilder(DelegationTokenImpl.SERVICE_NAME);
    if (id.getInstanceId() != null) {
      svcName.append("-").append(id.getInstanceId());
    }
    // Create password will update the state on the identifier given currentKey. Need to call this
    // before serializing the identifier
    byte[] password;
    try {
      password = createPassword(id, cfg);
    } catch (RuntimeException e) {
      throw new AccumuloException(e.getMessage());
    }
    // The use of the ServiceLoader inside Token doesn't work to automatically get the Identifier
    // Explicitly returning the identifier also saves an extra deserialization
    Token<AuthenticationTokenIdentifier> token =
        new Token<>(id.getBytes(), password, id.getKind(), new Text(svcName.toString()));
    return Maps.immutableEntry(token, id);
  }

  /**
   * Add the provided {@code key} to the in-memory copy of all {@link AuthenticationKey}s.
   *
   * @param key The key to add.
   */
  public synchronized void addKey(AuthenticationKey key) {
    requireNonNull(key);

    log.debug("Adding AuthenticationKey with keyId {}", key.getKeyId());

    allKeys.put(key.getKeyId(), key);
    if (currentKey == null || key.getKeyId() > currentKey.getKeyId()) {
      currentKey = key;
    }
  }

  /**
   * Removes the {@link AuthenticationKey} from the local cache of keys using the provided
   * {@code keyId}.
   *
   * @param keyId The unique ID for the {@link AuthenticationKey} to remove.
   * @return True if the key was removed, otherwise false.
   */
  synchronized boolean removeKey(Integer keyId) {
    requireNonNull(keyId);

    log.debug("Removing AuthenticationKey with keyId {}", keyId);

    return allKeys.remove(keyId) != null;
  }

  /**
   * The current {@link AuthenticationKey}, may be null.
   *
   * @return The current key, or null.
   */
  @VisibleForTesting
  AuthenticationKey getCurrentKey() {
    return currentKey;
  }

  @VisibleForTesting
  Map<Integer,AuthenticationKey> getKeys() {
    return allKeys;
  }

  /**
   * Inspect each key cached in {@link #allKeys} and remove it if the expiration date has passed.
   * For each removed local {@link AuthenticationKey}, the key is also removed from ZooKeeper using
   * the provided {@code keyDistributor} instance.
   *
   * @param keyDistributor ZooKeeper key distribution class
   */
  synchronized int removeExpiredKeys(ZooAuthenticationKeyDistributor keyDistributor) {
    long now = System.currentTimeMillis();
    int keysRemoved = 0;
    Iterator<Entry<Integer,AuthenticationKey>> iter = allKeys.entrySet().iterator();
    while (iter.hasNext()) {
      Entry<Integer,AuthenticationKey> entry = iter.next();
      AuthenticationKey key = entry.getValue();
      if (key.getExpirationDate() < now) {
        log.debug("Removing expired delegation token key {}", key.getKeyId());
        iter.remove();
        keysRemoved++;
        try {
          keyDistributor.remove(key);
        } catch (KeeperException | InterruptedException e) {
          log.error("Failed to remove AuthenticationKey from ZooKeeper. Exiting", e);
          throw new IllegalStateException(e);
        }
      }
    }
    return keysRemoved;
  }

  synchronized boolean isCurrentKeySet() {
    return currentKey != null;
  }

  /**
   * Atomic operation to remove all AuthenticationKeys
   */
  public synchronized void removeAllKeys() {
    allKeys.clear();
    currentKey = null;
  }

  @Override
  protected SecretKey generateSecret() {
    // Method in the parent is a different package, provide the explicit override so we can use it
    // directly in our package.
    return super.generateSecret();
  }

  @SuppressFBWarnings(value = "HSM_HIDING_METHOD", justification = "")
  public static SecretKey createSecretKey(byte[] raw) {
    return SecretManager.createSecretKey(raw);
  }
}
