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
package org.apache.accumulo.core.client.impl;

import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ClientInfo;
import org.apache.accumulo.core.client.Durability;
import org.apache.accumulo.core.conf.ClientProperty;

/**
 * Creates internal objects using {@link ClientInfo}
 */
public class ClientInfoFactory {

  public static String getString(ClientInfo info, ClientProperty property) {
    return property.getValue(info.getProperties());
  }

  public static Long getLong(ClientInfo info, ClientProperty property) {
    return property.getLong(info.getProperties());
  }

  public static AccumuloClient getClient(ClientInfo info)
      throws AccumuloSecurityException, AccumuloException {
    return new AccumuloClientImpl(new ClientContext(info));
  }

  public static Credentials getCredentials(ClientInfo info) {
    return new Credentials(info.getPrincipal(), info.getAuthenticationToken());
  }

  public static BatchWriterConfig getBatchWriterConfig(ClientInfo info) {
    BatchWriterConfig batchWriterConfig = new BatchWriterConfig();
    Long maxMemory = getLong(info, ClientProperty.BATCH_WRITER_MAX_MEMORY_BYTES);
    if (maxMemory != null) {
      batchWriterConfig.setMaxMemory(maxMemory);
    }
    Long maxLatency = getLong(info, ClientProperty.BATCH_WRITER_MAX_LATENCY_SEC);
    if (maxLatency != null) {
      batchWriterConfig.setMaxLatency(maxLatency, TimeUnit.SECONDS);
    }
    Long timeout = getLong(info, ClientProperty.BATCH_WRITER_MAX_TIMEOUT_SEC);
    if (timeout != null) {
      batchWriterConfig.setTimeout(timeout, TimeUnit.SECONDS);
    }
    String durability = getString(info, ClientProperty.BATCH_WRITER_DURABILITY);
    if (!durability.isEmpty()) {
      batchWriterConfig.setDurability(Durability.valueOf(durability.toUpperCase()));
    }
    return batchWriterConfig;
  }
}
