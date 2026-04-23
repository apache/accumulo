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
package org.apache.accumulo.core.rpc;

import org.apache.accumulo.core.lock.ServiceLockData;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;

/**
 * This is an enum containing all rpc service types used by Thrift. These are used by
 * {@link ThriftClientTypes} and {@link ServiceLockData} These services also contain a 1-byte
 * identifier to reduce RPC header size.
 */
public enum RpcService {

  CLIENT((byte) 0),
  COORDINATOR((byte) 1),
  COMPACTOR((byte) 2),
  FATE_CLIENT((byte) 3),
  FATE_WORKER((byte) 4),
  GC((byte) 5),
  MANAGER((byte) 6),
  NONE((byte) 7),
  TABLET_INGEST((byte) 8),
  TABLET_MANAGEMENT((byte) 9),
  TABLET_SCAN((byte) 10),
  TSERV((byte) 11),
  SERVER_PROCESS((byte) 12);

  private final byte shortID;

  private static final RpcService[] SERVICES = values();

  RpcService(byte shortID) {
    this.shortID = shortID;
  }

  public byte getShortId() {
    return this.shortID;
  }

  public static RpcService fromShortId(byte id) {
    for (RpcService service : SERVICES) {
      if (service.shortID == id) {
        return service;
      }
    }
    throw new IllegalArgumentException("Unknown RPC shortId: " + id);
  }
}
