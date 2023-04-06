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
package org.apache.accumulo.core.spi.crypto;

import java.util.Map;

import org.apache.accumulo.core.classloader.ClassLoaderUtil;

/**
 * A Factory that returns a CryptoService based on the environment and configuration.
 *
 * @since 2.1
 */
public interface CryptoServiceFactory {

  /**
   * Return the appropriate CryptoService.
   *
   * @param environment CryptoEnvironment containing a variety of information
   * @param properties configuration
   *
   * @return CryptoService based on the environment and configuration
   */
  CryptoService getService(CryptoEnvironment environment, Map<String,String> properties);

  /**
   * Loads a crypto service based on the name provided.
   */
  default CryptoService newCryptoService(String cryptoServiceName) {
    try {
      return ClassLoaderUtil.loadClass(null, cryptoServiceName, CryptoService.class)
          .getDeclaredConstructor().newInstance();
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }
}
