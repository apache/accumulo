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
package org.apache.accumulo.core.crypto;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.cryptoImpl.NoCryptoService;
import org.apache.accumulo.core.spi.crypto.CryptoService;

public class CryptoServiceFactory {

  public enum ClassloaderType {
    // Use the Accumulo custom classloader. Should only be used by Accumulo server side code.
    ACCUMULO,
    // Use basic Java classloading mechanism. Should be use by Accumulo client code.
    JAVA
  }

  public static CryptoService newInstance(AccumuloConfiguration conf, ClassloaderType ct) {

    CryptoService newCryptoService;

    if (ct == ClassloaderType.ACCUMULO) {
      newCryptoService = Property.createInstanceFromPropertyName(conf,
          Property.INSTANCE_CRYPTO_SERVICE, CryptoService.class, new NoCryptoService());
    } else if (ct == ClassloaderType.JAVA) {
      String clazzName = conf.get(Property.INSTANCE_CRYPTO_SERVICE);
      if (clazzName == null || clazzName.trim().isEmpty()) {
        newCryptoService = new NoCryptoService();
      } else {
        try {
          newCryptoService = CryptoServiceFactory.class.getClassLoader().loadClass(clazzName)
              .asSubclass(CryptoService.class).getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
          throw new RuntimeException(e);
        }
      }
    } else {
      throw new IllegalArgumentException();
    }

    newCryptoService.init(conf.getAllPropertiesWithPrefix(Property.INSTANCE_CRYPTO_PREFIX));
    return newCryptoService;
  }

  public static CryptoService newDefaultInstance() {
    return newInstance(DefaultConfiguration.getInstance(), ClassloaderType.JAVA);
  }
}
