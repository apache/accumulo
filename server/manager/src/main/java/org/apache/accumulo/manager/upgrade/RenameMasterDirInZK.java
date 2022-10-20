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

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.server.ServerContext;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A utility to handle the renaming of "/masters" to "/managers" in Zookeeper when upgrading from a
 * 2.0 (or earlier) to 2.1 instance. This utility is invoked automatically by
 * {@link org.apache.accumulo.manager.state.SetGoalState} (which normally runs first as a part of
 * accumulo startup scripts). However, if a user is not using the standard scripts or wishes to
 * perform the upgrade as a separate process, this utility can be invoked with:
 *
 * <pre>
 * {@code
 * bin/accumulo org.apache.accumulo.manager.upgrade.RenameMasterDirInZK
 * }
 * </pre>
 */
public class RenameMasterDirInZK {
  private static final Logger LOG = LoggerFactory.getLogger(RenameMasterDirInZK.class);

  public static void main(String[] args) {
    var ctx = new ServerContext(SiteConfiguration.auto());
    if (!renameMasterDirInZK(ctx)) {
      LOG.info(
          "Masters directory in ZooKeeper has already been renamed to managers. No action was taken.");
    }
  }

  public static boolean renameMasterDirInZK(ServerContext context) {
    final ZooReaderWriter zoo = context.getZooReaderWriter();
    final String mastersZooDir = context.getZooKeeperRoot() + "/masters";
    final String managersZooDir = context.getZooKeeperRoot() + Constants.ZMANAGERS;
    try {
      boolean mastersDirExists = zoo.exists(mastersZooDir);
      if (mastersDirExists) {
        LOG.info("Copying ZooKeeper directory {} to {}.", mastersZooDir, managersZooDir);
        zoo.recursiveCopyPersistentOverwrite(mastersZooDir, managersZooDir);
        LOG.info("Deleting ZooKeeper directory {}.", mastersZooDir);
        zoo.recursiveDelete(mastersZooDir, ZooUtil.NodeMissingPolicy.SKIP);
      }
      return mastersDirExists;
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException("Unable to rename " + mastersZooDir + " in ZooKeeper", e);
    }
  }
}
