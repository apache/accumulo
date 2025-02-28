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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.core.util.LazySingletons.GSON;

/**
 * Track upgrade progress for each component. The version stored is the most recent version for
 * which an upgrade has been completed.
 */
public class UpgradeProgress {

  int zooKeeperVersion;
  int rootVersion;
  int metadataVersion;
  int upgradeTargetVersion;

  public UpgradeProgress() {}

  public UpgradeProgress(int currentVersion, int targetVersion) {
    zooKeeperVersion = currentVersion;
    rootVersion = currentVersion;
    metadataVersion = currentVersion;
    upgradeTargetVersion = targetVersion;
  }

  public int getZooKeeperVersion() {
    return zooKeeperVersion;
  }

  public int getRootVersion() {
    return rootVersion;
  }

  public int getMetadataVersion() {
    return metadataVersion;
  }

  public int getUpgradeTargetVersion() {
    return upgradeTargetVersion;
  }

  public byte[] toJsonBytes() {
    return GSON.get().toJson(this).getBytes(UTF_8);
  }

  public static UpgradeProgress fromJsonBytes(byte[] jsonData) {
    return GSON.get().fromJson(new String(jsonData, UTF_8), UpgradeProgress.class);
  }

}
