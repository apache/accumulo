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

import static org.apache.accumulo.core.conf.Property.GENERAL_ARBITRARY_PROP_PREFIX;
import static org.apache.accumulo.core.conf.Property.TABLE_CRYPTO_PREFIX;

import java.util.Map;

/**
 * Factory that will load a crypto service configured, first checking
 * {@link #GENERAL_SERVICE_NAME_PROP} and then {@link #TABLE_SERVICE_NAME_PROP}. Useful for general
 * purpose on disk encryption, with no Table context.
 */
public class GenericCryptoServiceFactory implements CryptoServiceFactory {
  public static final String GENERAL_SERVICE_NAME_PROP =
      GENERAL_ARBITRARY_PROP_PREFIX + "crypto.service";
  public static final String TABLE_SERVICE_NAME_PROP = TABLE_CRYPTO_PREFIX + "service";

  @Override
  public CryptoService getService(CryptoEnvironment environment, Map<String,String> properties) {
    if (properties == null || properties.isEmpty()) {
      return NoCryptoServiceFactory.NONE;
    }

    String cryptoServiceName = properties.get(GENERAL_SERVICE_NAME_PROP);
    if (cryptoServiceName == null) {
      cryptoServiceName = properties.get(TABLE_SERVICE_NAME_PROP);
      if (cryptoServiceName == null) {
        return NoCryptoServiceFactory.NONE;
      }
    }
    var cs = newCryptoService(cryptoServiceName);
    cs.init(properties);
    return cs;
  }
}
