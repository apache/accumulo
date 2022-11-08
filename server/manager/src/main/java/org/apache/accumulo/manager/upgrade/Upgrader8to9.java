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
package org.apache.accumulo.manager.upgrade;

import org.apache.accumulo.server.AccumuloDataVersion;
import org.apache.accumulo.server.ServerContext;

/**
 * See {@link AccumuloDataVersion#CRYPTO_CHANGES}
 */
public class Upgrader8to9 implements Upgrader {

  @Override
  public void upgradeZookeeper(ServerContext context) {
    // There is no action that needs to be taken for zookeeper
  }

  @Override
  public void upgradeRoot(ServerContext context) {
    // There is no action that needs to be taken for metadata
  }

  @Override
  public void upgradeMetadata(ServerContext context) {
    // There is no action that needs to be taken for metadata
  }

}
