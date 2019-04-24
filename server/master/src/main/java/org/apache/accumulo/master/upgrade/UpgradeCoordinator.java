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

package org.apache.accumulo.master.upgrade;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.ServerUtil;

public interface UpgradeCoordinator {

  void upgradeZookeeper();

  Future<Void> upgradeMetadata();

  public static UpgradeCoordinator create(ServerContext ctx) {
    final int accumuloPersistentVersion =
        ServerUtil.getAccumuloPersistentVersion(ctx.getVolumeManager());

    ServerUtil.ensureDataVersionCompatible(accumuloPersistentVersion);

    if (ServerUtil.persistentVersionNeedsUpgrade(accumuloPersistentVersion)) {
      return new UpgradeCoordinatorImpl(accumuloPersistentVersion, ctx);
    } else {
      // No upgrade needed so return a no-op coordinator
      return new UpgradeCoordinator() {
        @Override
        public void upgradeZookeeper() {}

        @Override
        public Future<Void> upgradeMetadata() {
          return CompletableFuture.completedFuture(null);
        }
      };
    }
  }

}
