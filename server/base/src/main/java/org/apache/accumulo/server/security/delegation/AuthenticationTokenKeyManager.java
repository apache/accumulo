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

import java.util.List;

import org.apache.accumulo.core.util.Daemon;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * Service that handles generation of the secret key used to create delegation tokens.
 */
public class AuthenticationTokenKeyManager extends Daemon {
  private static final Logger log = LoggerFactory.getLogger(AuthenticationTokenKeyManager.class);

  private final AuthenticationTokenSecretManager secretManager;
  private final ZooAuthenticationKeyDistributor keyDistributor;

  private long lastKeyUpdate = 0;
  private long keyUpdateInterval;
  private long tokenMaxLifetime;
  private int idSeq = 0;
  private volatile boolean keepRunning = true, initialized = false;

  /**
   * Construct the key manager which will generate new AuthenticationKeys to generate and verify delegation tokens
   *
   * @param mgr
   *          The SecretManager in use
   * @param dist
   *          The implementation to distribute AuthenticationKeys to ZooKeeper
   * @param keyUpdateInterval
   *          The frequency, in milliseconds, that new AuthenticationKeys are created
   * @param tokenMaxLifetime
   *          The lifetime, in milliseconds, of generated AuthenticationKeys (and subsequently delegation tokens).
   */
  public AuthenticationTokenKeyManager(AuthenticationTokenSecretManager mgr, ZooAuthenticationKeyDistributor dist, long keyUpdateInterval, long tokenMaxLifetime) {
    super("Delegation Token Key Manager");
    this.secretManager = mgr;
    this.keyDistributor = dist;
    this.keyUpdateInterval = keyUpdateInterval;
    this.tokenMaxLifetime = tokenMaxLifetime;
  }

  @VisibleForTesting
  void setKeepRunning(boolean keepRunning) {
    this.keepRunning = keepRunning;
  }

  public boolean isInitialized() {
    return initialized;
  }

  public void gracefulStop() {
    keepRunning = false;
  }

  @Override
  public void run() {
    // Make sure to initialize the secret manager with keys already in ZK
    updateStateFromCurrentKeys();
    initialized = true;

    while (keepRunning) {
      long now = System.currentTimeMillis();

      _run(now);

      try {
        Thread.sleep(5000);
      } catch (InterruptedException ie) {
        log.debug("Interrupted waiting for next update", ie);
      }
    }
  }

  @VisibleForTesting
  void updateStateFromCurrentKeys() {
    try {
      List<AuthenticationKey> currentKeys = keyDistributor.getCurrentKeys();
      if (!currentKeys.isEmpty()) {
        for (AuthenticationKey key : currentKeys) {
          // Ensure that we don't create new Keys with duplicate keyIds for keys that already exist
          // It's not a big concern if we happen to duplicate keyIds for already expired keys.
          if (key.getKeyId() > idSeq) {
            idSeq = key.getKeyId();
          }
          secretManager.addKey(key);
        }
        log.info("Added {} existing AuthenticationKeys into the local cache from ZooKeeper", currentKeys.size());

        // Try to use the last key instead of creating a new one right away. This will present more expected
        // functionality if the active master happens to die for some reasonn
        AuthenticationKey currentKey = secretManager.getCurrentKey();
        if (null != currentKey) {
          log.info("Updating last key update to {} from current secret manager key", currentKey.getCreationDate());
          lastKeyUpdate = currentKey.getCreationDate();
        }
      }
    } catch (KeeperException | InterruptedException e) {
      log.warn("Failed to fetch existing AuthenticationKeys from ZooKeeper");
    }
  }

  @VisibleForTesting
  long getLastKeyUpdate() {
    return lastKeyUpdate;
  }

  @VisibleForTesting
  int getIdSeq() {
    return idSeq;
  }

  /**
   * Internal "run" method which performs the actual work.
   *
   * @param now
   *          The current time in millis since epoch.
   */
  void _run(long now) {
    // clear any expired keys
    int removedKeys = secretManager.removeExpiredKeys(keyDistributor);
    if (removedKeys > 0) {
      log.debug("Removed {} expired keys from the local cache", removedKeys);
    }

    if (lastKeyUpdate + keyUpdateInterval < now) {
      log.debug("Key update interval passed, creating new authentication key");

      // Increment the idSeq and use the new value as the unique ID
      AuthenticationKey newKey = new AuthenticationKey(++idSeq, now, now + tokenMaxLifetime, secretManager.generateSecret());

      log.debug("Created new {}", newKey.toString());

      // Will set to be the current key given the idSeq
      secretManager.addKey(newKey);

      // advertise it to tabletservers
      try {
        keyDistributor.advertise(newKey);
      } catch (KeeperException | InterruptedException e) {
        log.error("Failed to advertise AuthenticationKey in ZooKeeper. Exiting.", e);
        throw new RuntimeException(e);
      }

      lastKeyUpdate = now;
    }
  }
}
