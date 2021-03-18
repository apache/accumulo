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

import static org.apache.accumulo.core.conf.Property.TABLE_CRYPTO_DECRYPT_SERVICES;
import static org.apache.accumulo.core.conf.Property.TSERV_WALOG_CRYPTO_DECRYPT_SERVICE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.spi.crypto.CryptoService;
import org.apache.accumulo.core.spi.crypto.FileEncrypter;
import org.apache.accumulo.core.spi.crypto.NoCryptoService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CryptoServiceFactory {
  private static final Logger log = LoggerFactory.getLogger(CryptoServiceFactory.class);

  /**
   * Create a new CryptoService for RFiles. Pulls from table config, calls init and returns the
   * loaded CryptoService.
   */
  public static FileEncrypter newRFileInstance(AccumuloConfiguration conf, ClassloaderType ct) {
    CryptoService.Scope scope = CryptoService.Scope.RFILE;
    var initParams = new FileEncrypter.InitParams() {
      @Override
      public Map<String,String> getOptions() {
        return conf.getAllPropertiesWithPrefixStripped(Property.TABLE_CRYPTO_PREFIX);
      }

      @Override
      public CryptoService.Scope getScope() {
        return scope;
      }
    };
    return newInstance(scope, initParams, conf, ct);
  }

  public enum ClassloaderType {
    // Use the Accumulo custom classloader. Should only be used by Accumulo server side code.
    ACCUMULO,
    // Use basic Java classloading mechanism. Should be use by Accumulo client code.
    JAVA
  }

  /**
   * Create a new CryptoService based on the scope and conf provided. Class loading technique
   * determined by the type provided. Calls the init method using the provided initParams and
   * returns the FileEncrypter.
   */
  public static FileEncrypter newInstance(CryptoService.Scope scope,
      FileEncrypter.InitParams initParams, AccumuloConfiguration conf, ClassloaderType ct) {
    CryptoService newCryptoService;
    Objects.requireNonNull(scope, "CryptoService Scope required");
    Property prop = CryptoUtils.getPropPerScope(scope);

    if (ct == ClassloaderType.ACCUMULO) {
      newCryptoService = Property.createInstanceFromPropertyName(conf, prop, CryptoService.class,
          new NoCryptoService());
    } else if (ct == ClassloaderType.JAVA) {
      String clazzName = conf.get(prop);
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

    var encrypter = newCryptoService.getEncrypter();
    encrypter.init(initParams);
    return encrypter;
  }

  public static List<CryptoService> getDecrypters(AccumuloConfiguration conf, ClassloaderType ct) {
    String[] classes = conf.get(TABLE_CRYPTO_DECRYPT_SERVICES).split(",");
    ArrayList<CryptoService> decrypters = new ArrayList<>();
    if (classes.length == 0 || classes[0].equals(NoCryptoService.class.getName()))
      return decrypters;

    try {
      for (String c : classes) {
        CryptoService cs = NoCryptoService.NONE;
        if (ct == ClassloaderType.ACCUMULO) {
          cs = ConfigurationTypeHelper.getClassInstance(null, c, CryptoService.class);
        } else if (ct == ClassloaderType.JAVA) {
          if (c != null && !c.trim().isEmpty()) {
            try {
              cs = CryptoServiceFactory.class.getClassLoader().loadClass(c)
                  .asSubclass(CryptoService.class).getDeclaredConstructor().newInstance();
            } catch (ReflectiveOperationException e) {
              throw new RuntimeException(e);
            }
          }
        } else {
          throw new IllegalArgumentException();
        }
        decrypters.add(cs);
      }
    } catch (ReflectiveOperationException | IOException e) {
      log.warn("Failed to load class from property {}", TABLE_CRYPTO_DECRYPT_SERVICES, e);
    }
    return decrypters;
  }

  public static CryptoService getWALDecrypter(AccumuloConfiguration conf) {
    String clazz = conf.get(TSERV_WALOG_CRYPTO_DECRYPT_SERVICE);
    CryptoService cs = NoCryptoService.NONE;
    try {
      cs = ConfigurationTypeHelper.getClassInstance(null, clazz, CryptoService.class);
    } catch (ReflectiveOperationException | IOException e) {
      log.warn("Failed to load class from property {}", TSERV_WALOG_CRYPTO_DECRYPT_SERVICE, e);
    }
    return cs;
  }
}
