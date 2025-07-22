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
package org.apache.accumulo.server.conf.store;

import static org.apache.accumulo.core.Constants.ZCONFIG;
import static org.apache.accumulo.core.Constants.ZRESOURCEGROUPS;

import java.io.IOException;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.core.zookeeper.ZooSession;
import org.apache.accumulo.server.conf.codec.VersionedPropCodec;
import org.apache.accumulo.server.conf.codec.VersionedProperties;
import org.apache.zookeeper.KeeperException;

public class ResourceGroupPropKey extends IdBasedPropStoreKey<ResourceGroupId> {

  public static final ResourceGroupPropKey DEFAULT =
      ResourceGroupPropKey.of(ResourceGroupId.DEFAULT);

  private ResourceGroupPropKey(final ResourceGroupId id) {
    super(ZRESOURCEGROUPS + "/" + id.canonical() + ZCONFIG, id);
  }

  public static ResourceGroupPropKey of(final ResourceGroupId id) {
    return new ResourceGroupPropKey(id);
  }

  public void removeZNode(ZooSession zoo) throws KeeperException, InterruptedException {
    zoo.asReaderWriter().recursiveDelete(Constants.ZRESOURCEGROUPS + "/" + getId().canonical(),
        NodeMissingPolicy.SKIP);
  }

  public void createZNode(ZooReaderWriter zrw) throws KeeperException, InterruptedException {

    zrw.mkdirs(Constants.ZRESOURCEGROUPS + "/" + getId().canonical());

    var rgPropPath = getPath();
    if (zrw.exists(rgPropPath)) {
      return;
    }
    try {
      boolean created = zrw.putPrivatePersistentData(rgPropPath,
          VersionedPropCodec.getDefault().toBytes(new VersionedProperties()),
          ZooUtil.NodeExistsPolicy.FAIL);
      if (!created) {
        throw new IllegalStateException(
            "Failed to create default resource group props during initialization at: "
                + rgPropPath);
      }
    } catch (IOException e) {
      throw new IllegalStateException(
          "Failed to create default resource group props during initialization at: " + rgPropPath,
          e);
    }

  }
}
