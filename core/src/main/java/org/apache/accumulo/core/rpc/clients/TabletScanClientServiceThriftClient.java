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
package org.apache.accumulo.core.rpc.clients;

import org.apache.accumulo.core.tabletscan.thrift.TabletScanClientService.Client;

/**
 * Client side object that can be used to interact with services that support scan operations
 * against tablets. See {@link TabletScanClientService$Iface} for a list of supported operations.
 */
public class TabletScanClientServiceThriftClient extends ThriftClientTypes<Client> {

  TabletScanClientServiceThriftClient(String serviceName) {
    super(serviceName, new Client.Factory());
  }

}
