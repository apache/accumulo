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

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.start.classloader.vfs.AccumuloVFSClassLoader;

public class CryptoServiceFactory {
  private static CryptoService singleton = null;

  /**
   * Load the singleton class configured in {@link Property#TABLE_CRYPTO_SERVICE}
   */
  public static CryptoService getConfigured(AccumuloConfiguration conf) {
    String configuredClass = conf.get(Property.TABLE_CRYPTO_SERVICE.getKey());
    if (singleton == null) {
      singleton = loadCryptoService(configuredClass);
      singleton.init(conf.getAllPropertiesWithPrefix(Property.TABLE_PREFIX));
    } else {
      if (!singleton.getClass().getName().equals(configuredClass)) {
        singleton = loadCryptoService(configuredClass);
        singleton.init(conf.getAllPropertiesWithPrefix(Property.TABLE_PREFIX));
      }
    }
    return singleton;
  }

  private static CryptoService loadCryptoService(String className) {
    try {
      Class<? extends CryptoService> clazz = AccumuloVFSClassLoader.loadClass(className,
          CryptoService.class);
      return clazz.newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
