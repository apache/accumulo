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
package org.apache.accumulo.core.conf;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.Objects;

import org.apache.accumulo.core.security.crypto.CryptoModule;
import org.apache.accumulo.core.security.crypto.SecretKeyEncryptionStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * A utility class for validating {@link AccumuloConfiguration} instances.
 */
public class ConfigSanityCheck {

  private static final Logger log = LoggerFactory.getLogger(ConfigSanityCheck.class);
  private static final String PREFIX = "BAD CONFIG ";
  private static final String NULL_CIPHER = "NullCipher";
  private static final String NULL_CRYPTO_MODULE = "NullCryptoModule";
  private static final String NULL_SECRET_KEY_ENCRYPTION_STRATEGY = "NullSecretKeyEncryptionStrategy";
  @SuppressWarnings("deprecation")
  private static final Property INSTANCE_DFS_URI = Property.INSTANCE_DFS_URI;
  @SuppressWarnings("deprecation")
  private static final Property INSTANCE_DFS_DIR = Property.INSTANCE_DFS_DIR;

  /**
   * Validates the given configuration entries. A valid configuration contains only valid properties (i.e., defined or otherwise valid) that are not prefixes
   * and whose values are formatted correctly for their property types. A valid configuration also contains a value for property
   * {@link Property#INSTANCE_ZK_TIMEOUT} within a valid range.
   *
   * @param entries
   *          iterable through configuration keys and values
   * @throws SanityCheckException
   *           if a fatal configuration error is found
   */
  public static void validate(Iterable<Entry<String,String>> entries) {
    String instanceZkTimeoutValue = null;
    boolean usingVolumes = false;
    String cipherSuite = NULL_CIPHER;
    String keyAlgorithm = NULL_CIPHER;
    String secretKeyEncryptionStrategy = NULL_SECRET_KEY_ENCRYPTION_STRATEGY;
    String cryptoModule = NULL_CRYPTO_MODULE;
    for (Entry<String,String> entry : entries) {
      String key = entry.getKey();
      String value = entry.getValue();
      Property prop = Property.getPropertyByKey(entry.getKey());
      if (prop == null && Property.isValidPropertyKey(key))
        continue; // unknown valid property (i.e. has proper prefix)
      else if (prop == null)
        log.warn(PREFIX + "unrecognized property key (" + key + ")");
      else if (prop.getType() == PropertyType.PREFIX)
        fatal(PREFIX + "incomplete property key (" + key + ")");
      else if (!prop.getType().isValidFormat(value))
        fatal(PREFIX + "improperly formatted value for key (" + key + ", type=" + prop.getType() + ") : " + value);

      if (key.equals(Property.INSTANCE_ZK_TIMEOUT.getKey())) {
        instanceZkTimeoutValue = value;
      }

      if (key.equals(Property.INSTANCE_VOLUMES.getKey())) {
        usingVolumes = value != null && !value.isEmpty();
      }

      // If the block size or block size index is configured to be too large, we throw an exception to avoid potentially corrupting RFiles later
      if (key.equals(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE_INDEX.getKey()) || key.equals(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE.getKey())) {
        long bsize = ConfigurationTypeHelper.getFixedMemoryAsBytes(value);
        Preconditions.checkArgument(bsize > 0 && bsize < Integer.MAX_VALUE, key + " must be greater than 0 and less than " + Integer.MAX_VALUE + " but was: "
            + bsize);
      }

      if (key.equals(Property.CRYPTO_CIPHER_SUITE.getKey())) {
        cipherSuite = Objects.requireNonNull(value);
        Preconditions.checkArgument(cipherSuite.equals(NULL_CIPHER) || cipherSuite.split("/").length == 3,
            "Cipher suite must be NullCipher or in the form algorithm/mode/padding. Suite: " + cipherSuite + " is invalid.");
      }

      if (key.equals(Property.CRYPTO_CIPHER_KEY_ALGORITHM_NAME.getKey())) {
        keyAlgorithm = Objects.requireNonNull(value);
      }

      if (key.equals(Property.CRYPTO_MODULE_CLASS.getKey())) {
        cryptoModule = Objects.requireNonNull(value);
        if (!cryptoModule.equals(NULL_CRYPTO_MODULE)) {
          verifyValidClassName(key, cryptoModule, CryptoModule.class);
        }

      }
      if (key.equals(Property.CRYPTO_SECRET_KEY_ENCRYPTION_STRATEGY_CLASS.getKey())) {
        secretKeyEncryptionStrategy = Objects.requireNonNull(value);
        if (!secretKeyEncryptionStrategy.equals(NULL_SECRET_KEY_ENCRYPTION_STRATEGY)) {
          verifyValidClassName(key, secretKeyEncryptionStrategy, SecretKeyEncryptionStrategy.class);
        }
      }
    }

    if (instanceZkTimeoutValue != null) {
      checkTimeDuration(Property.INSTANCE_ZK_TIMEOUT, instanceZkTimeoutValue, new CheckTimeDurationBetween(1000, 300000));
    }

    if (!usingVolumes) {
      log.warn("Use of {} and {} are deprecated. Consider using {} instead.", INSTANCE_DFS_URI, INSTANCE_DFS_DIR, Property.INSTANCE_VOLUMES);
    }

    if ((cipherSuite.equals(NULL_CIPHER) || keyAlgorithm.equals(NULL_CIPHER)) && !cipherSuite.equals(keyAlgorithm)) {
      fatal(Property.CRYPTO_CIPHER_SUITE.getKey() + " and " + Property.CRYPTO_CIPHER_KEY_ALGORITHM_NAME + " must both be configured.");
    }

    if (cryptoModule.equals(NULL_CRYPTO_MODULE) ^ secretKeyEncryptionStrategy.equals(NULL_SECRET_KEY_ENCRYPTION_STRATEGY)) {
      fatal(Property.CRYPTO_MODULE_CLASS.getKey() + " and " + Property.CRYPTO_SECRET_KEY_ENCRYPTION_STRATEGY_CLASS.getKey() + " must both be configured.");
    }
  }

  private interface CheckTimeDuration {
    boolean check(long propVal);

    String getDescription(Property prop);
  }

  private static class CheckTimeDurationBetween implements CheckTimeDuration {
    long min, max;

    CheckTimeDurationBetween(long x, long y) {
      min = Math.min(x, y);
      max = Math.max(x, y);
    }

    @Override
    public boolean check(long propVal) {
      return propVal >= min && propVal <= max;
    }

    @Override
    public String getDescription(Property prop) {
      return "ensure " + min + " <= " + prop + " <= " + max;
    }
  }

  private static void checkTimeDuration(Property prop, String value, CheckTimeDuration chk) {
    verifyPropertyTypes(PropertyType.TIMEDURATION, prop);
    if (!chk.check(ConfigurationTypeHelper.getTimeInMillis(value)))
      fatal(PREFIX + chk.getDescription(prop));
  }

  private static void verifyPropertyTypes(PropertyType type, Property... properties) {
    for (Property prop : properties)
      if (prop.getType() != type)
        fatal("Unexpected property type (" + prop.getType() + " != " + type + ")");
  }

  /**
   * The exception thrown when {@link ConfigSanityCheck#validate(Iterable)} fails.
   */
  public static class SanityCheckException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new exception with the given message.
     */
    public SanityCheckException(String msg) {
      super(msg);
    }
  }

  private static void fatal(String msg) {
    // ACCUMULO-3651 Level changed from fatal to error and FATAL added to message for slf4j compatibility
    log.error("FATAL: {}", msg);
    throw new SanityCheckException(msg);
  }

  /**
   * Verifies a configured option is a legal class and has a required base class.
   *
   * @param confOption
   *          The Property key name
   * @param className
   *          The Property value, the string representation of a class to be loaded
   * @param requiredBaseClass
   *          The base class required for the className
   */
  private static void verifyValidClassName(String confOption, String className, Class<?> requiredBaseClass) {
    try {
      ConfigurationTypeHelper.getClassInstance(null, className, requiredBaseClass);
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | IOException e) {
      fatal(confOption + " has an invalid class name: " + className);
    } catch (ClassCastException e) {
      fatal(confOption + " must implement " + requiredBaseClass + ", but the configured class does not: " + className);
    }
  }
}
