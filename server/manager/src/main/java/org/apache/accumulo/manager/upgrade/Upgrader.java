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
package org.apache.accumulo.manager.upgrade;

import org.apache.accumulo.server.ServerContext;

/**
 * The purpose of this interface is to allow per version upgrade implementations to be created.
 * Keeping the code for upgrading each version separate makes it easier to maintain and understand
 * the upgrade code over time.
 *
 * <p>
 * Upgrade operations should be idempotent. For failure cases upgrade operations may partially
 * complete and then be run again later.
 */
public interface Upgrader {
  void upgradeZookeeper(ServerContext ctx);

  void upgradeRoot(ServerContext ctx);

  void upgradeMetadata(ServerContext ctx);

  void upgradeFiles(ServerContext ctx);
}
