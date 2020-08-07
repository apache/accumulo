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
package org.apache.accumulo.master.upgrade;

import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.ServerContext;

/**
 * See {@link ServerConstants#CRYPTO_CHANGES}
 */
public class Upgrader8to9 implements Upgrader {

  @Override
  public void upgradeZookeeper(ServerContext ctx) {
    // There is no action that needs to be taken for zookeeper
  }

  @Override
  public void upgradeRoot(ServerContext ctx) {
    // There is no action that needs to be taken for metadata
  }

  @Override
  public void upgradeMetadata(ServerContext ctx) {
    // There is no action that needs to be taken for metadata
  }

}
