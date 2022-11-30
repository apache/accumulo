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
package org.apache.accumulo.core.crypto;

import static org.apache.accumulo.core.crypto.CryptoFactoryLoader.ClassloaderType.ACCUMULO;
import static org.apache.accumulo.core.crypto.CryptoFactoryLoader.ClassloaderType.JAVA;
import static org.apache.accumulo.core.spi.crypto.CryptoEnvironment.Scope.TABLE;

import java.util.Map;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.spi.crypto.CryptoEnvironment;
import org.apache.accumulo.core.spi.crypto.CryptoService;
import org.apache.accumulo.core.spi.crypto.CryptoServiceFactory;
import org.apache.accumulo.core.spi.crypto.GenericCryptoServiceFactory;
import org.apache.accumulo.core.spi.crypto.NoCryptoServiceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CryptoFactoryLoader {
  private static final Logger log = LoggerFactory.getLogger(CryptoFactoryLoader.class);
  private static final CryptoServiceFactory NO_CRYPTO_FACTORY = new NoCryptoServiceFactory();

  enum ClassloaderType {
    // Use the Accumulo custom classloader. Should only be used by Accumulo server side code.
    ACCUMULO,
    // Use basic Java classloading mechanism. Should be use by Accumulo client code.
    JAVA
  }

  /**
   * Creates a new server Factory.
   */
  public static CryptoServiceFactory newInstance(AccumuloConfiguration conf) {
    String clazzName = conf.get(Property.INSTANCE_CRYPTO_FACTORY);
    return loadCryptoFactory(ACCUMULO, clazzName);
  }

  /**
   * For use by server utilities not associated with a table. Requires Instance, general and table
   * configuration. Creates a new Factory from the configuration and gets the CryptoService from
   * that Factory.
   */
  public static CryptoService getServiceForServer(AccumuloConfiguration conf) {
    var env = new CryptoEnvironmentImpl(TABLE, null, null);
    CryptoServiceFactory factory = newInstance(conf);
    var allCryptoProperties = conf.getAllCryptoProperties();
    return factory.getService(env, allCryptoProperties);
  }

  /**
   * Returns a CryptoService configured for the scope using the properties. This is used for client
   * operations not associated with a table, either for r-files (TABLE scope) or WALs. The
   * GenericCryptoServiceFactory is used for loading the CryptoService.
   */
  public static CryptoService getServiceForClient(CryptoEnvironment.Scope scope,
      Map<String,String> properties) {
    var factory = loadCryptoFactory(JAVA, GenericCryptoServiceFactory.class.getName());
    CryptoEnvironment env = new CryptoEnvironmentImpl(scope, null, null);
    return factory.getService(env, properties);
  }

  /**
   * For use by client code, in a Table context.
   */
  public static CryptoService getServiceForClientWithTable(Map<String,String> systemConfig,
      Map<String,String> tableProps, TableId tableId) {
    String factoryKey = Property.INSTANCE_CRYPTO_FACTORY.getKey();
    String clazzName = systemConfig.get(factoryKey);
    if (clazzName == null || clazzName.trim().isEmpty()) {
      return NoCryptoServiceFactory.NONE;
    }

    var env = new CryptoEnvironmentImpl(TABLE, tableId, null);
    CryptoServiceFactory factory = loadCryptoFactory(JAVA, clazzName);
    return factory.getService(env, tableProps);
  }

  private static CryptoServiceFactory loadCryptoFactory(ClassloaderType ct, String clazzName) {
    log.debug("Creating new crypto factory class {}", clazzName);
    CryptoServiceFactory newCryptoServiceFactory;
    if (ct == ACCUMULO) {
      newCryptoServiceFactory = ConfigurationTypeHelper.getClassInstance(null, clazzName,
          CryptoServiceFactory.class, new NoCryptoServiceFactory());
    } else if (ct == JAVA) {
      if (clazzName == null || clazzName.trim().isEmpty()) {
        newCryptoServiceFactory = NO_CRYPTO_FACTORY;
      } else {
        try {
          newCryptoServiceFactory = CryptoFactoryLoader.class.getClassLoader().loadClass(clazzName)
              .asSubclass(CryptoServiceFactory.class).getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
          throw new RuntimeException(e);
        }
      }
    } else {
      throw new IllegalArgumentException();
    }
    return newCryptoServiceFactory;
  }
}
