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

import static java.util.Objects.requireNonNull;
import static org.apache.accumulo.core.conf.Property.GENERAL_ARBITRARY_PROP_PREFIX;
import static org.apache.accumulo.core.conf.Property.TABLE_CRYPTO_PREFIX;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.accumulo.core.data.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A factory that loads a CryptoService based on {@link TableId}.
 */
public class PerTableCryptoServiceFactory implements CryptoServiceFactory {
  private static final Logger log = LoggerFactory.getLogger(PerTableCryptoServiceFactory.class);
  private final ConcurrentHashMap<TableId,CryptoService> cryptoServiceMap =
      new ConcurrentHashMap<>();

  public static final String WAL_NAME_PROP = GENERAL_ARBITRARY_PROP_PREFIX + "crypto.wal.service";
  public static final String RECOVERY_NAME_PROP =
      GENERAL_ARBITRARY_PROP_PREFIX + "crypto.recovery.service";
  public static final String TABLE_SERVICE_NAME_PROP = TABLE_CRYPTO_PREFIX + "service";
  // The WALs do not have table IDs so these fake IDs are used for caching
  private static final TableId WAL_FAKE_ID = TableId.of("WAL_CryptoService_FAKE_ID");
  private static final TableId REC_FAKE_ID = TableId.of("RECOVERY_CryptoService_FAKE_ID");

  @Override
  public CryptoService getService(CryptoEnvironment environment, Map<String,String> props) {
    if (environment.getScope() == CryptoEnvironment.Scope.WAL) {
      return cryptoServiceMap.computeIfAbsent(WAL_FAKE_ID, (id) -> getWALServiceInitialized(props));
    } else if (environment.getScope() == CryptoEnvironment.Scope.RECOVERY) {
      return cryptoServiceMap.computeIfAbsent(REC_FAKE_ID,
          (id) -> getRecoveryServiceInitialized(props));
    } else {
      if (environment.getTableId().isEmpty()) {
        log.debug("No tableId present in crypto env: " + environment);
        return NoCryptoServiceFactory.NONE;
      }
      TableId tableId = environment.getTableId().get();
      if (props == null || props.isEmpty() || props.get(TABLE_SERVICE_NAME_PROP) == null) {
        return NoCryptoServiceFactory.NONE;
      }
      if (environment.getScope() == CryptoEnvironment.Scope.TABLE) {
        return cryptoServiceMap.computeIfAbsent(tableId,
            (id) -> getTableServiceInitialized(tableId, props));
      }
    }
    throw new IllegalStateException("Invalid config for crypto " + environment + " " + props);
  }

  private CryptoService getWALServiceInitialized(Map<String,String> props) {
    String name = requireNonNull(props.get(WAL_NAME_PROP),
        "The property " + WAL_NAME_PROP + " is required for encrypting WALs.");
    log.debug("New CryptoService for WAL scope {}={}", WAL_NAME_PROP, name);
    CryptoService cs = newCryptoService(name);
    cs.init(props);
    return cs;
  }

  private CryptoService getRecoveryServiceInitialized(Map<String,String> props) {
    String name = requireNonNull(props.get(RECOVERY_NAME_PROP),
        "The property " + RECOVERY_NAME_PROP + " is required for encrypting during recovery.");
    log.debug("New CryptoService for Recovery scope {}={}", RECOVERY_NAME_PROP, name);
    CryptoService cs = newCryptoService(name);
    cs.init(props);
    return cs;
  }

  private CryptoService getTableServiceInitialized(TableId tableId, Map<String,String> props) {
    String name = requireNonNull(props.get(TABLE_SERVICE_NAME_PROP),
        "The property " + TABLE_SERVICE_NAME_PROP + " is required for encrypting tables.");
    log.debug("New CryptoService for TABLE({}) {}={}", tableId, TABLE_SERVICE_NAME_PROP, name);
    CryptoService cs = newCryptoService(name);
    cs.init(props);
    return cs;
  }

  public int getCount() {
    return cryptoServiceMap.size();
  }
}
