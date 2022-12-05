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

  /**
   * Update entries in ZooKeeper - called before the root tablet is loaded.
   *
   * @param context the server context.
   */
  void upgradeZookeeper(ServerContext context);

  /**
   * Update the root tablet - called after the root tablet is loaded and before the metadata table
   * is loaded.
   *
   * @param context the server context.
   */
  void upgradeRoot(ServerContext context);

  /**
   * Update the metadata table - called after the metadata table is loaded and before loading user
   * tablets.
   *
   * @param context the server context.
   */
  void upgradeMetadata(ServerContext context);
}
