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

import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.lock.ServiceLockData;

/**
 * This is an enum containing all rpc service types used by Thrift.
 * These are used by {@link ThriftClientTypes} and {@link ServiceLockData}
 */
public enum RpcService {
  CLIENT,
  COORDINATOR,
  COMPACTOR,
  FATE_CLIENT,
  FATE_WORKER,
  GC,
  MANAGER,
  NONE,
  TABLET_INGEST,
  TABLET_MANAGEMENT,
  TABLET_SCAN,
  TSERV,
  SERVER_PROCESS
}
