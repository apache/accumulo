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
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.spi.crypto.CryptoService;
import org.apache.accumulo.core.spi.crypto.CryptoService.CryptoException;
import org.apache.accumulo.start.classloader.vfs.AccumuloVFSClassLoader;

public class CryptoServiceFactory {
  /**
   * Load and initialize the CryptoService only once, when this class is loaded.
   */
  private static CryptoService singleton = init();

  private static CryptoService init() {
    SiteConfiguration conf = SiteConfiguration.getInstance();
    String configuredClass = conf.get(Property.INSTANCE_CRYPTO_SERVICE.getKey());
    CryptoService newCryptoService = loadCryptoService(configuredClass);
    newCryptoService.init(conf.getAllPropertiesWithPrefix(Property.INSTANCE_CRYPTO_PREFIX));
    return newCryptoService;
  }

  /**
   * Get the class configured in {@link Property#INSTANCE_CRYPTO_SERVICE}. This class should have
   * been loaded and initialized when CryptoServiceFactory is loaded.
   *
   * @throws CryptoException
   *           if class configured differs from the original class loaded
   */
  public static CryptoService getConfigured(AccumuloConfiguration conf) {
    String currentClass = singleton.getClass().getName();
    String configuredClass = conf.get(Property.INSTANCE_CRYPTO_SERVICE.getKey());
    if (!currentClass.equals(configuredClass)) {
      String msg = String.format("Configured crypto class %s changed since initialization of %s.",
          configuredClass, currentClass);
      throw new CryptoService.CryptoException(msg);
    }
    return singleton;
  }

  private static CryptoService loadCryptoService(String className) {
    try {
      Class<? extends CryptoService> clazz = AccumuloVFSClassLoader.loadClass(className,
          CryptoService.class);
      return clazz.newInstance();
    } catch (Exception e) {
      throw new CryptoException(e);
    }
  }
}
