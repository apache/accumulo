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
package org.apache.accumulo.server.init;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.server.init.Initialize.REPL_TABLE_ID;

import java.io.IOException;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.clientImpl.Namespace;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.core.manager.thrift.ManagerGoalState;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.RootTabletMetadata;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooUtil;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.codec.VersionedPropCodec;
import org.apache.accumulo.server.conf.codec.VersionedProperties;
import org.apache.accumulo.server.conf.store.PropCacheKey;
import org.apache.accumulo.server.log.WalStateManager;
import org.apache.accumulo.server.metadata.RootGcCandidates;
import org.apache.accumulo.server.tables.TableManager;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;

class ZooKeeperInitializer {

  private final byte[] EMPTY_BYTE_ARRAY = new byte[0];
  private final byte[] ZERO_CHAR_ARRAY = {'0'};

  /**
   * The prop store requires that the system config node exists so that it can set watchers. This
   * methods creates the ZooKeeper nodes /accumulo/INSTANCE_ID/config/encoded_props and sets the
   * encoded_props to a default, empty node if it does not exist. If any of the paths or the
   * encoded_props node exist, the are skipped and not modified.
   *
   * @param instanceId
   *          the instance id
   * @param zoo
   *          a ZooReaderWriter
   */
  void initializeConfig(final InstanceId instanceId, final ZooReaderWriter zoo) {
    try {

      zoo.putPersistentData(Constants.ZROOT, new byte[0], ZooUtil.NodeExistsPolicy.SKIP,
          ZooDefs.Ids.OPEN_ACL_UNSAFE);

      String zkInstanceRoot = Constants.ZROOT + "/" + instanceId;
      zoo.putPersistentData(zkInstanceRoot, EMPTY_BYTE_ARRAY, ZooUtil.NodeExistsPolicy.SKIP);
      zoo.putPersistentData(zkInstanceRoot + Constants.ZCONFIG, EMPTY_BYTE_ARRAY,
          ZooUtil.NodeExistsPolicy.SKIP);

      var sysPropPath = PropCacheKey.forSystem(instanceId).getPath();
      VersionedProperties vProps = new VersionedProperties();
      // skip if the encoded props node exists
      if (zoo.exists(sysPropPath)) {
        return;
      }
      var created = zoo.putPersistentData(sysPropPath,
          VersionedPropCodec.getDefault().toBytes(vProps), ZooUtil.NodeExistsPolicy.FAIL);
      if (!created) {
        throw new IllegalStateException(
            "Failed to create default system props during initialization at: {}" + sysPropPath);
      }
    } catch (IOException | KeeperException | InterruptedException ex) {
      throw new IllegalStateException("Failed to initialize configuration for prop store", ex);
    }
  }

  void initialize(final ServerContext context, final boolean clearInstanceName,
      final String instanceNamePath, final String rootTabletDirName, final String rootTabletFileUri)
      throws KeeperException, InterruptedException {
    // setup basic data in zookeeper

    ZooReaderWriter zoo = context.getZooReaderWriter();
    InstanceId instanceId = context.getInstanceID();

    zoo.putPersistentData(Constants.ZROOT + Constants.ZINSTANCES, new byte[0],
        ZooUtil.NodeExistsPolicy.SKIP, ZooDefs.Ids.OPEN_ACL_UNSAFE);

    // setup instance name
    if (clearInstanceName) {
      zoo.recursiveDelete(instanceNamePath, ZooUtil.NodeMissingPolicy.SKIP);
    }
    zoo.putPersistentData(instanceNamePath, instanceId.canonical().getBytes(UTF_8),
        ZooUtil.NodeExistsPolicy.FAIL);

    // setup the instance
    String zkInstanceRoot = Constants.ZROOT + "/" + instanceId;
    zoo.putPersistentData(zkInstanceRoot + Constants.ZTABLES, Constants.ZTABLES_INITIAL_ID,
        ZooUtil.NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + Constants.ZNAMESPACES, new byte[0],
        ZooUtil.NodeExistsPolicy.FAIL);

    TableManager.prepareNewNamespaceState(context, Namespace.DEFAULT.id(), Namespace.DEFAULT.name(),
        ZooUtil.NodeExistsPolicy.FAIL);
    TableManager.prepareNewNamespaceState(context, Namespace.ACCUMULO.id(),
        Namespace.ACCUMULO.name(), ZooUtil.NodeExistsPolicy.FAIL);

    TableManager.prepareNewTableState(context, RootTable.ID, Namespace.ACCUMULO.id(),
        RootTable.NAME, TableState.ONLINE, ZooUtil.NodeExistsPolicy.FAIL);
    TableManager.prepareNewTableState(context, MetadataTable.ID, Namespace.ACCUMULO.id(),
        MetadataTable.NAME, TableState.ONLINE, ZooUtil.NodeExistsPolicy.FAIL);
    @SuppressWarnings("deprecation")
    String replicationTableName = org.apache.accumulo.core.replication.ReplicationTable.NAME;
    TableManager.prepareNewTableState(context, REPL_TABLE_ID, Namespace.ACCUMULO.id(),
        replicationTableName, TableState.OFFLINE, ZooUtil.NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + Constants.ZTSERVERS, EMPTY_BYTE_ARRAY,
        ZooUtil.NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + Constants.ZPROBLEMS, EMPTY_BYTE_ARRAY,
        ZooUtil.NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + RootTable.ZROOT_TABLET,
        RootTabletMetadata.getInitialJson(rootTabletDirName, rootTabletFileUri),
        ZooUtil.NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + RootTable.ZROOT_TABLET_GC_CANDIDATES,
        new RootGcCandidates().toJson().getBytes(UTF_8), ZooUtil.NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + Constants.ZMANAGERS, EMPTY_BYTE_ARRAY,
        ZooUtil.NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + Constants.ZMANAGER_LOCK, EMPTY_BYTE_ARRAY,
        ZooUtil.NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + Constants.ZMANAGER_GOAL_STATE,
        ManagerGoalState.NORMAL.toString().getBytes(UTF_8), ZooUtil.NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + Constants.ZGC, EMPTY_BYTE_ARRAY,
        ZooUtil.NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + Constants.ZGC_LOCK, EMPTY_BYTE_ARRAY,
        ZooUtil.NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + Constants.ZTABLE_LOCKS, EMPTY_BYTE_ARRAY,
        ZooUtil.NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + Constants.ZHDFS_RESERVATIONS, EMPTY_BYTE_ARRAY,
        ZooUtil.NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + Constants.ZNEXT_FILE, ZERO_CHAR_ARRAY,
        ZooUtil.NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + Constants.ZRECOVERY, ZERO_CHAR_ARRAY,
        ZooUtil.NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + Constants.ZMONITOR, EMPTY_BYTE_ARRAY,
        ZooUtil.NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + Constants.ZMONITOR_LOCK, EMPTY_BYTE_ARRAY,
        ZooUtil.NodeExistsPolicy.FAIL);
    @SuppressWarnings("deprecation")
    String replicationZBase = org.apache.accumulo.core.replication.ReplicationConstants.ZOO_BASE;
    zoo.putPersistentData(zkInstanceRoot + replicationZBase, EMPTY_BYTE_ARRAY,
        ZooUtil.NodeExistsPolicy.FAIL);
    @SuppressWarnings("deprecation")
    String replicationZServers =
        org.apache.accumulo.core.replication.ReplicationConstants.ZOO_TSERVERS;
    zoo.putPersistentData(zkInstanceRoot + replicationZServers, EMPTY_BYTE_ARRAY,
        ZooUtil.NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + WalStateManager.ZWALS, EMPTY_BYTE_ARRAY,
        ZooUtil.NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + Constants.ZCOORDINATOR, EMPTY_BYTE_ARRAY,
        ZooUtil.NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + Constants.ZCOORDINATOR_LOCK, EMPTY_BYTE_ARRAY,
        ZooUtil.NodeExistsPolicy.FAIL);
    zoo.putPersistentData(zkInstanceRoot + Constants.ZCOMPACTORS, EMPTY_BYTE_ARRAY,
        ZooUtil.NodeExistsPolicy.FAIL);

  }

}
