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

import java.util.Objects;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.server.AccumuloDataVersion;
import org.apache.accumulo.server.ServerContext;
import org.apache.zookeeper.KeeperException;

import com.google.common.base.Preconditions;

public class UpgradeProgressTracker {

  public static class ComponentVersions {
    private int zooKeeperVersion;
    private int rootVersion;
    private int metadataVersion;

    int getZooKeeperVersion() {
      return zooKeeperVersion;
    }

    synchronized void updateZooKeeperVersion(ServerContext context, int newVersion)
        throws KeeperException, InterruptedException {
      Objects.requireNonNull(context, "ServerContext must be supplied");
      Preconditions.checkArgument(newVersion > zooKeeperVersion,
          "new version must be greater than current version");
      zooKeeperVersion = newVersion;
      put(context, this);
    }

    int getRootVersion() {
      return rootVersion;
    }

    synchronized void updateRootVersion(ServerContext context, int newVersion)
        throws KeeperException, InterruptedException {
      Objects.requireNonNull(context, "ServerContext must be supplied");
      Preconditions.checkArgument(newVersion > rootVersion,
          "new version must be greater than current version");
      rootVersion = newVersion;
      put(context, this);
    }

    int getMetadataVersion() {
      return metadataVersion;
    }

    synchronized void updateMetadataVersion(ServerContext context, int newVersion)
        throws KeeperException, InterruptedException {
      Objects.requireNonNull(context, "ServerContext must be supplied");
      Preconditions.checkArgument(newVersion > metadataVersion,
          "new version must be greater than current version");
      metadataVersion = newVersion;
      put(context, this);
    }
  }

  private static String getZPath(ServerContext context) {
    return context.getZooKeeperRoot() + Constants.ZUPGRADE_STATUS;
  }

  private static synchronized void put(ServerContext context, ComponentVersions cv)
      throws KeeperException, InterruptedException {
    final String zpath = getZPath(context);
    final ZooReaderWriter zrw = context.getZooSession().asReaderWriter();
    zrw.sync(zpath);
    if (!zrw.exists(zpath)) {
      zrw.mkdirs(zpath);
    }
    zrw.putPersistentData(zpath, GSON.get().toJson(cv).getBytes(UTF_8), NodeExistsPolicy.OVERWRITE);
  }

  static synchronized ComponentVersions get(ServerContext context)
      throws KeeperException, InterruptedException {
    final String zpath = getZPath(context);
    final int currentVersion = AccumuloDataVersion.getCurrentVersion(context);
    final ZooReaderWriter zrw = context.getZooSession().asReaderWriter();
    zrw.sync(zpath);
    if (!zrw.exists(zpath)) {
      ComponentVersions cv = new ComponentVersions();
      cv.zooKeeperVersion = currentVersion;
      cv.rootVersion = currentVersion;
      cv.metadataVersion = currentVersion;
      put(context, cv);
      return cv;
    } else {
      byte[] jsonData = zrw.getData(zpath);
      return GSON.get().fromJson(new String(jsonData, UTF_8), ComponentVersions.class);
    }
  }

  static synchronized void upgradeComplete(ServerContext context)
      throws KeeperException, InterruptedException {
    final String zpath = getZPath(context);
    final ZooReaderWriter zrw = context.getZooSession().asReaderWriter();
    zrw.sync(zpath);
    if (!zrw.exists(zpath)) {
      zrw.recursiveDelete(zpath, NodeMissingPolicy.SKIP);
    }
  }

}
