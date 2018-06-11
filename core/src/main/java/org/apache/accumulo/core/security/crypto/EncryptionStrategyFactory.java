/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.accumulo.core.security.crypto;

import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.start.classloader.vfs.AccumuloVFSClassLoader;

public class EncryptionStrategyFactory {

  /**
   * Load and initialize EncryptionStrategy read from file. Check strategy read from file matches
   * the same strategy that is configured, otherwise throw RuntimeException.
   *
   * @param fileEncryptedClass
   *          encryption strategy class read from the file
   * @return EncryptionStrategy
   * @throws IOException
   *           if an error occurred during EncryptionStrategy initialization
   */
  public static EncryptionStrategy setupReadEncryption(Map<String,String> conf,
      String fileEncryptedClass, EncryptionStrategy.Scope scope) throws IOException {
    String confCryptoStrategyClass = conf.get(Property.CRYPTO_STRATEGY);
    if (!fileEncryptedClass.equals(confCryptoStrategyClass)) {
      throw new RuntimeException("File encrypted with different encryption (" + fileEncryptedClass
          + ") than what is configured: " + confCryptoStrategyClass);
    }
    return initEncryption(conf, loadStrategy(confCryptoStrategyClass), scope);
  }

  /**
   * Load and initialize configured EncryptionStrategy.
   *
   * @return EncryptionStrategy
   * @throws IOException
   *           if an error occurred during EncryptionStrategy initialization
   */
  public static EncryptionStrategy setupConfiguredEncryption(Map<String,String> conf,
      EncryptionStrategy.Scope scope) throws IOException {
    String name = conf.get(Property.CRYPTO_STRATEGY);
    return initEncryption(conf, loadStrategy(name), scope);
  }

  private static EncryptionStrategy initEncryption(Map<String,String> conf,
      EncryptionStrategy strategy, EncryptionStrategy.Scope scope) throws IOException {
    try {
      if (!strategy.init(scope, conf)) {
        strategy = new NoEncryptionStrategy();
      }
    } catch (Exception e) {
      throw new IOException("Error initializing Encryption", e);
    }
    return strategy;
  }

  private static EncryptionStrategy loadStrategy(String className) {
    try {
      Class<? extends EncryptionStrategy> clazz = AccumuloVFSClassLoader.loadClass(className,
          EncryptionStrategy.class);
      return clazz.newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
