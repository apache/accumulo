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
package org.apache.accumulo.core.security.crypto;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.start.classloader.vfs.AccumuloVFSClassLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This factory module exists to assist other classes in loading crypto modules.
 *
 *
 */
public class CryptoModuleFactory {

  private static final Logger log = LoggerFactory.getLogger(CryptoModuleFactory.class);
  private static final Map<String,CryptoModule> cryptoModulesCache = new HashMap<>();
  private static final Map<String,SecretKeyEncryptionStrategy> secretKeyEncryptionStrategyCache = new HashMap<>();

  /**
   * This method returns a crypto module based on settings in the given configuration parameter.
   *
   * @return a class implementing the CryptoModule interface. It will *never* return null; rather, it will return a class which obeys the interface but makes no
   *         changes to the underlying data.
   */
  public static CryptoModule getCryptoModule(AccumuloConfiguration conf) {
    String cryptoModuleClassname = conf.get(Property.CRYPTO_MODULE_CLASS);
    return getCryptoModule(cryptoModuleClassname);
  }

  public static CryptoModule getCryptoModule(String cryptoModuleClassname) {

    if (cryptoModuleClassname != null) {
      cryptoModuleClassname = cryptoModuleClassname.trim();
    }

    if (cryptoModuleClassname == null || cryptoModuleClassname.equals("NullCryptoModule")) {
      return new NullCryptoModule();
    }

    CryptoModule cryptoModule = null;
    synchronized (cryptoModulesCache) {
      if (cryptoModulesCache.containsKey(cryptoModuleClassname)) {
        cryptoModule = cryptoModulesCache.get(cryptoModuleClassname);
      } else {
        cryptoModule = instantiateCryptoModule(cryptoModuleClassname);
        cryptoModulesCache.put(cryptoModuleClassname, cryptoModule);
      }
    }

    return cryptoModule;
  }

  private static CryptoModule instantiateCryptoModule(String cryptoModuleClassname) {
    log.debug("About to instantiate crypto module {}", cryptoModuleClassname);

    CryptoModule cryptoModule = null;
    Class<?> cryptoModuleClazz = null;
    try {
      cryptoModuleClazz = AccumuloVFSClassLoader.loadClass(cryptoModuleClassname);
    } catch (ClassNotFoundException e1) {
      throw new IllegalArgumentException("Could not find configured crypto module " + cryptoModuleClassname);
    }

    // Check if the given class implements the CryptoModule interface
    Class<?>[] interfaces = cryptoModuleClazz.getInterfaces();
    boolean implementsCryptoModule = false;

    for (Class<?> clazz : interfaces) {
      if (clazz.equals(CryptoModule.class)) {
        implementsCryptoModule = true;
        break;
      }
    }

    if (!implementsCryptoModule) {
      throw new IllegalArgumentException("Configured Accumulo crypto module " + cryptoModuleClassname + " does not implement the CryptoModule interface.");
    } else {
      try {
        cryptoModule = (CryptoModule) cryptoModuleClazz.newInstance();

        log.debug("Successfully instantiated crypto module {}", cryptoModuleClassname);

      } catch (InstantiationException | IllegalAccessException e) {
        throw new IllegalArgumentException("Unable to instantiate the crypto module: " + cryptoModuleClassname, e);
      }
    }
    return cryptoModule;
  }

  public static SecretKeyEncryptionStrategy getSecretKeyEncryptionStrategy(AccumuloConfiguration conf) {
    String className = conf.get(Property.CRYPTO_SECRET_KEY_ENCRYPTION_STRATEGY_CLASS);
    return getSecretKeyEncryptionStrategy(className);
  }

  public static SecretKeyEncryptionStrategy getSecretKeyEncryptionStrategy(String className) {

    if (className != null) {
      className = className.trim();
    }

    if (className == null || className.equals("NullSecretKeyEncryptionStrategy")) {
      return new NullSecretKeyEncryptionStrategy();
    }

    SecretKeyEncryptionStrategy strategy = null;
    synchronized (secretKeyEncryptionStrategyCache) {
      if (secretKeyEncryptionStrategyCache.containsKey(className)) {
        strategy = secretKeyEncryptionStrategyCache.get(className);
      } else {
        strategy = instantiateSecreteKeyEncryptionStrategy(className);
        secretKeyEncryptionStrategyCache.put(className, strategy);
      }
    }

    return strategy;
  }

  private static SecretKeyEncryptionStrategy instantiateSecreteKeyEncryptionStrategy(String className) {

    log.debug("About to instantiate secret key encryption strategy {}", className);

    SecretKeyEncryptionStrategy strategy = null;
    Class<?> keyEncryptionStrategyClazz = null;
    try {
      keyEncryptionStrategyClazz = AccumuloVFSClassLoader.loadClass(className);
    } catch (ClassNotFoundException e1) {
      throw new IllegalArgumentException("Could not find configured secret key encryption strategy: " + className);
    }

    // Check if the given class implements the CryptoModule interface
    Class<?>[] interfaces = keyEncryptionStrategyClazz.getInterfaces();
    boolean implementsSecretKeyStrategy = false;

    for (Class<?> clazz : interfaces) {
      if (clazz.equals(SecretKeyEncryptionStrategy.class)) {
        implementsSecretKeyStrategy = true;
        break;
      }
    }

    if (!implementsSecretKeyStrategy) {
      throw new IllegalArgumentException(
          "Configured Accumulo secret key encryption strategy \"%s\" does not implement the SecretKeyEncryptionStrategy interface.");
    } else {
      try {
        strategy = (SecretKeyEncryptionStrategy) keyEncryptionStrategyClazz.newInstance();

        log.debug("Successfully instantiated secret key encryption strategy {}", className);

      } catch (InstantiationException | IllegalAccessException e) {
        throw new IllegalArgumentException("Unable to instantiate the secret key encryption strategy: " + className, e);
      }
    }
    return strategy;
  }

  static class NullSecretKeyEncryptionStrategy implements SecretKeyEncryptionStrategy {

    @Override
    public CryptoModuleParameters encryptSecretKey(CryptoModuleParameters params) {
      params.setEncryptedKey(params.getPlaintextKey());
      params.setOpaqueKeyEncryptionKeyID("");

      return params;
    }

    @Override
    public CryptoModuleParameters decryptSecretKey(CryptoModuleParameters params) {
      params.setPlaintextKey(params.getEncryptedKey());
      return params;
    }

  }

  static class NullCryptoModule implements CryptoModule {

    @Override
    public CryptoModuleParameters getEncryptingOutputStream(CryptoModuleParameters params) throws IOException {
      params.setEncryptedOutputStream(params.getPlaintextOutputStream());
      return params;
    }

    @Override
    public CryptoModuleParameters getDecryptingInputStream(CryptoModuleParameters params) throws IOException {
      params.setPlaintextInputStream(params.getEncryptedInputStream());
      return params;
    }

    @Override
    public CryptoModuleParameters generateNewRandomSessionKey(CryptoModuleParameters params) {
      params.setPlaintextKey(new byte[0]);
      return params;
    }

    @Override
    public CryptoModuleParameters initializeCipher(CryptoModuleParameters params) {
      return params;
    }

  }

  public static CryptoModuleParameters createParamsObjectFromAccumuloConfiguration(AccumuloConfiguration conf) {
    CryptoModuleParameters params = new CryptoModuleParameters();

    return fillParamsObjectFromConfiguration(params, conf);
  }

  public static CryptoModuleParameters fillParamsObjectFromConfiguration(CryptoModuleParameters params, AccumuloConfiguration conf) {
    // Get all the options from the configuration
    Map<String,String> cryptoOpts = new HashMap<>(conf.getAllPropertiesWithPrefix(Property.CRYPTO_PREFIX));
    cryptoOpts.putAll(conf.getAllPropertiesWithPrefix(Property.INSTANCE_PREFIX));
    cryptoOpts.remove(Property.INSTANCE_SECRET.getKey());
    cryptoOpts.put(Property.CRYPTO_BLOCK_STREAM_SIZE.getKey(), Integer.toString((int) conf.getAsBytes(Property.CRYPTO_BLOCK_STREAM_SIZE)));

    return fillParamsObjectFromStringMap(params, cryptoOpts);
  }

  public static CryptoModuleParameters fillParamsObjectFromStringMap(CryptoModuleParameters params, Map<String,String> cryptoOpts) {
    params.setCipherSuite(cryptoOpts.get(Property.CRYPTO_CIPHER_SUITE.getKey()));
    // If no encryption has been specified, then we abort here.
    if (params.getCipherSuite() == null || params.getCipherSuite().equals("NullCipher")) {
      params.setAllOptions(cryptoOpts);

      return params;
    }

    params.setAllOptions(cryptoOpts);

    params.setKeyAlgorithmName(cryptoOpts.get(Property.CRYPTO_CIPHER_KEY_ALGORITHM_NAME.getKey()));
    params.setKeyEncryptionStrategyClass(cryptoOpts.get(Property.CRYPTO_SECRET_KEY_ENCRYPTION_STRATEGY_CLASS.getKey()));
    params.setKeyLength(Integer.parseInt(cryptoOpts.get(Property.CRYPTO_CIPHER_KEY_LENGTH.getKey())));
    params.setOverrideStreamsSecretKeyEncryptionStrategy(Boolean.parseBoolean(cryptoOpts.get(Property.CRYPTO_OVERRIDE_KEY_STRATEGY_WITH_CONFIGURED_STRATEGY
        .getKey())));
    params.setRandomNumberGenerator(cryptoOpts.get(Property.CRYPTO_SECURE_RNG.getKey()));
    params.setRandomNumberGeneratorProvider(cryptoOpts.get(Property.CRYPTO_SECURE_RNG_PROVIDER.getKey()));
    params.setSecurityProvider(cryptoOpts.get(Property.CRYPTO_SECURITY_PROVIDER.getKey()));
    String blockStreamSize = cryptoOpts.get(Property.CRYPTO_BLOCK_STREAM_SIZE.getKey());
    if (blockStreamSize != null)
      params.setBlockStreamSize(Integer.parseInt(blockStreamSize));

    return params;
  }

}
