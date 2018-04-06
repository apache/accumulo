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

import java.util.Map;

import org.apache.accumulo.core.conf.ConfigSanityCheck.SanityCheckException;
import org.junit.Before;
import org.junit.Test;

public class ConfigSanityCheckTest {
  private Map<String,String> m;

  // These are used when a valid class is needed for testing
  private static final String PROPS_PREFIX = "org.apache.accumulo.core.security.crypto.";
  private static final String DEFAULT_CRYPTO_MODULE = PROPS_PREFIX + "DefaultCryptoModule";
  private static final String DEFAULT_SECRET_KEY_ENCRYPTION_STRATEGY = PROPS_PREFIX
      + "NonCachingSecretKeyEncryptionStrategy";

  @Before
  public void setUp() {
    m = new java.util.HashMap<>();
  }

  @Test
  public void testPass() {
    m.put(Property.MASTER_CLIENTPORT.getKey(), "9999");
    m.put(Property.MASTER_TABLET_BALANCER.getKey(),
        "org.apache.accumulo.server.master.balancer.TableLoadBalancer");
    m.put(Property.MASTER_RECOVERY_MAXAGE.getKey(), "60m");
    m.put(Property.MASTER_BULK_RETRIES.getKey(), "3");
    ConfigSanityCheck.validate(m.entrySet());
  }

  @Test
  public void testPass_Empty() {
    ConfigSanityCheck.validate(m.entrySet());
  }

  @Test
  public void testPass_UnrecognizedValidProperty() {
    m.put(Property.MASTER_CLIENTPORT.getKey(), "9999");
    m.put(Property.MASTER_PREFIX.getKey() + "something", "abcdefg");
    ConfigSanityCheck.validate(m.entrySet());
  }

  @Test
  public void testPass_UnrecognizedProperty() {
    m.put(Property.MASTER_CLIENTPORT.getKey(), "9999");
    m.put("invalid.prefix.value", "abcdefg");
    ConfigSanityCheck.validate(m.entrySet());
  }

  @Test(expected = SanityCheckException.class)
  public void testFail_Prefix() {
    m.put(Property.MASTER_CLIENTPORT.getKey(), "9999");
    m.put(Property.MASTER_PREFIX.getKey(), "oops");
    ConfigSanityCheck.validate(m.entrySet());
  }

  @Test(expected = SanityCheckException.class)
  public void testFail_InvalidFormat() {
    m.put(Property.MASTER_CLIENTPORT.getKey(), "9999");
    m.put(Property.MASTER_RECOVERY_MAXAGE.getKey(), "60breem");
    ConfigSanityCheck.validate(m.entrySet());
  }

  @Test(expected = SanityCheckException.class)
  public void testFail_InstanceZkTimeoutOutOfRange() {
    m.put(Property.INSTANCE_ZK_TIMEOUT.getKey(), "10ms");
    ConfigSanityCheck.validate(m.entrySet());
  }

  @Test(expected = SanityCheckException.class)
  public void testFail_cipherSuiteSetKeyAlgorithmNotSet() {
    m.put(Property.CRYPTO_CIPHER_SUITE.getKey(), "AES/CBC/NoPadding");
    m.put(Property.CRYPTO_CIPHER_KEY_ALGORITHM_NAME.getKey(), "NullCipher");
    ConfigSanityCheck.validate(m.entrySet());
  }

  @Test(expected = SanityCheckException.class)
  public void testFail_cipherSuiteNotSetKeyAlgorithmSet() {
    m.put(Property.CRYPTO_CIPHER_SUITE.getKey(), "NullCipher");
    m.put(Property.CRYPTO_CIPHER_KEY_ALGORITHM_NAME.getKey(), "AES");
    ConfigSanityCheck.validate(m.entrySet());
  }

  @Test(expected = SanityCheckException.class)
  public void testFail_cryptoModuleInvalidClass() {
    // a random hex dump is unlikely to be a real class name
    m.put(Property.CRYPTO_MODULE_CLASS.getKey(), "e0218734bcd1e4d239203f970806786b");
    m.put(Property.CRYPTO_SECRET_KEY_ENCRYPTION_STRATEGY_CLASS.getKey(),
        DEFAULT_SECRET_KEY_ENCRYPTION_STRATEGY);
    ConfigSanityCheck.validate(m.entrySet());
  }

  @Test(expected = SanityCheckException.class)
  public void testFail_cryptoModuleValidClassNotValidInterface() {
    m.put(Property.CRYPTO_MODULE_CLASS.getKey(), "java.lang.String");
    m.put(Property.CRYPTO_SECRET_KEY_ENCRYPTION_STRATEGY_CLASS.getKey(),
        DEFAULT_SECRET_KEY_ENCRYPTION_STRATEGY);
    ConfigSanityCheck.validate(m.entrySet());
  }

  @Test
  public void testPass_cryptoModuleAndSecretKeyEncryptionStrategyValidClasses() {
    m.put(Property.CRYPTO_MODULE_CLASS.getKey(), DEFAULT_CRYPTO_MODULE);
    m.put(Property.CRYPTO_SECRET_KEY_ENCRYPTION_STRATEGY_CLASS.getKey(),
        DEFAULT_SECRET_KEY_ENCRYPTION_STRATEGY);
    ConfigSanityCheck.validate(m.entrySet());
  }

  @Test
  public void testPass_cryptoModuleValidNullModule() {
    m.put(Property.CRYPTO_MODULE_CLASS.getKey(), "NullCryptoModule");
    ConfigSanityCheck.validate(m.entrySet());
  }

  @Test(expected = SanityCheckException.class)
  public void testFail_secretKeyEncryptionStrategyInvalidClass() {
    // a random hex dump is unlikely to be a real class name
    m.put(Property.CRYPTO_SECRET_KEY_ENCRYPTION_STRATEGY_CLASS.getKey(),
        "e0218734bcd1e4d239203f970806786b");
    m.put(Property.CRYPTO_MODULE_CLASS.getKey(), DEFAULT_CRYPTO_MODULE);
    ConfigSanityCheck.validate(m.entrySet());
  }

  @Test(expected = SanityCheckException.class)
  public void testFail_secretKeyEncryptionStrategyValidClassNotValidInterface() {
    m.put(Property.CRYPTO_SECRET_KEY_ENCRYPTION_STRATEGY_CLASS.getKey(), "java.lang.String");
    m.put(Property.CRYPTO_MODULE_CLASS.getKey(), DEFAULT_CRYPTO_MODULE);
    ConfigSanityCheck.validate(m.entrySet());
  }

  @Test
  public void testPass_secretKeyEncryptionStrategyValidNullStrategy() {
    m.put(Property.CRYPTO_SECRET_KEY_ENCRYPTION_STRATEGY_CLASS.getKey(),
        "NullSecretKeyEncryptionStrategy");
    ConfigSanityCheck.validate(m.entrySet());
  }

  @Test(expected = SanityCheckException.class)
  public void testFail_cryptoModuleSetSecretKeyEncryptionStrategyNotSet() {
    m.put(Property.CRYPTO_MODULE_CLASS.getKey(), DEFAULT_CRYPTO_MODULE);
    m.put(Property.CRYPTO_SECRET_KEY_ENCRYPTION_STRATEGY_CLASS.getKey(),
        "NullSecretKeyEncryptionStrategy");
    ConfigSanityCheck.validate(m.entrySet());
  }

  @Test(expected = SanityCheckException.class)
  public void testFail_cryptoModuleNotSetSecretKeyEncryptionStrategySet() {
    m.put(Property.CRYPTO_MODULE_CLASS.getKey(), "NullCryptoModule");
    m.put(Property.CRYPTO_SECRET_KEY_ENCRYPTION_STRATEGY_CLASS.getKey(),
        DEFAULT_SECRET_KEY_ENCRYPTION_STRATEGY);
    ConfigSanityCheck.validate(m.entrySet());
  }

  @Test
  public void testPass_cryptoModuleAndSecretKeyEncryptionStrategyBothNull() {
    m.put(Property.CRYPTO_MODULE_CLASS.getKey(), "NullCryptoModule");
    m.put(Property.CRYPTO_SECRET_KEY_ENCRYPTION_STRATEGY_CLASS.getKey(),
        "NullSecretKeyEncryptionStrategy");
    ConfigSanityCheck.validate(m.entrySet());
  }

  @Test
  public void testPass_cryptoModuleAndSecretKeyEncryptionStrategyBothSet() {
    m.put(Property.CRYPTO_MODULE_CLASS.getKey(), DEFAULT_CRYPTO_MODULE);
    m.put(Property.CRYPTO_SECRET_KEY_ENCRYPTION_STRATEGY_CLASS.getKey(),
        DEFAULT_SECRET_KEY_ENCRYPTION_STRATEGY);
    ConfigSanityCheck.validate(m.entrySet());
  }
}
