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
import static org.apache.accumulo.core.Constants.ZNAMESPACES;
import static org.apache.accumulo.core.Constants.ZTABLES;
import static org.apache.accumulo.core.Constants.ZTABLE_STATE;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.RESERVED_PREFIX;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.volume.Volume;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.store.NamespacePropKey;
import org.apache.accumulo.server.conf.store.PropStore;
import org.apache.accumulo.server.conf.store.PropStoreKey;
import org.apache.accumulo.server.conf.store.SystemPropKey;
import org.apache.accumulo.server.conf.store.TablePropKey;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

public class Upgrader10to11 implements Upgrader {

  private static final Logger log = LoggerFactory.getLogger(Upgrader10to11.class);

  // Included for upgrade code usage any other usage post 3.0 should not be used.
  private static final TableId REPLICATION_ID = TableId.of("+rep");

  public Upgrader10to11() {
    super();
  }

  @Override
  public void upgradeZookeeper(final ServerContext context) {
    log.info("upgrade of ZooKeeper entries");

    var zrw = context.getZooReaderWriter();
    var iid = context.getInstanceID();

    // if the replication base path (../tables/+rep) assume removed or never existed.
    if (!checkReplicationTableInZk(iid, zrw)) {
      log.debug("replication table root node does not exist in ZooKeeper - nothing to do");
      return;
    }

    // if the replication table is online - stop. There could be data in transit.
    if (!checkReplicationOffline(iid, zrw)) {
      throw new IllegalStateException(
          "Replication table is not offline. Cannot continue with upgrade that will remove replication with replication active");
    }

    deleteReplicationConfigs(zrw, iid, context.getPropStore());

    deleteReplicationTableZkEntries(zrw, iid);

  }

  @Override
  public void upgradeRoot(final ServerContext context) {
    log.info("upgrade root - skipping, nothing to do");
  }

  @Override
  public void upgradeMetadata(final ServerContext context) {
    log.info("upgrade metadata entries");
    deleteReplMetadataEntries(context);
    deleteReplHdfsFiles(context);
  }

  /**
   * remove +rep entries from metadata.
   */
  private void deleteReplMetadataEntries(final ServerContext context) {
    try (BatchDeleter deleter =
        context.createBatchDeleter(MetadataTable.NAME, Authorizations.EMPTY, 10)) {

      Range repTableRange =
          new Range(REPLICATION_ID.canonical() + ";", true, REPLICATION_ID.canonical() + "<", true);
      // copied from MetadataSchema 2.1 (removed in 3.0)
      Range repWalRange =
          new Range(RESERVED_PREFIX + "repl", true, RESERVED_PREFIX + "repm", false);

      deleter.setRanges(List.of(repTableRange, repWalRange));
      deleter.delete();
    } catch (TableNotFoundException | MutationsRejectedException ex) {
      throw new IllegalStateException("failed to remove replication info from metadata table", ex);
    }
  }

  @VisibleForTesting
  void deleteReplHdfsFiles(final ServerContext context) {
    try {
      for (Volume volume : context.getVolumeManager().getVolumes()) {
        String dirUri = volume.getBasePath() + Constants.HDFS_TABLES_DIR + Path.SEPARATOR
            + REPLICATION_ID.canonical();
        Path replPath = new Path(dirUri);
        if (volume.getFileSystem().exists(replPath)) {
          try {
            log.debug("Removing replication dir and files in hdfs {}", replPath);
            volume.getFileSystem().delete(replPath, true);
          } catch (IOException ex) {
            log.error("Unable to remove replication dir and files from " + replPath + ": " + ex);
          }
        }
      }
    } catch (IOException ex) {
      log.error("Unable to remove replication dir and files: " + ex);
    }
  }

  private boolean checkReplicationTableInZk(final InstanceId iid, final ZooReaderWriter zrw) {
    try {
      String path = buildRepTablePath(iid);
      return zrw.exists(path);
    } catch (KeeperException ex) {
      throw new IllegalStateException("ZooKeeper error - cannot determine replication table status",
          ex);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("interrupted reading replication state from ZooKeeper", ex);
    }
  }

  /**
   * To protect against removing replication information if replication is being used and possible
   * active, check the replication table state in Zookeeper to see if it is ONLINE (active) or
   * OFFLINE (inactive). If the state node does not exist, then the status is considered as OFFLINE.
   *
   * @return true if the replication table state is OFFLINE, false otherwise
   */
  private boolean checkReplicationOffline(final InstanceId iid, final ZooReaderWriter zrw) {
    try {
      String path = buildRepTablePath(iid) + ZTABLE_STATE;
      byte[] bytes = zrw.getData(path);
      if (bytes != null && bytes.length > 0) {
        String status = new String(bytes, UTF_8);
        return TableState.OFFLINE.name().equals(status);
      }
      return false;
    } catch (KeeperException ex) {
      throw new IllegalStateException("ZooKeeper error - cannot determine replication table status",
          ex);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("interrupted reading replication state from ZooKeeper", ex);
    }
  }

  /**
   * Utility method to build the ZooKeeper replication table path. The path resolves to
   * {@code /accumulo/INSTANCE_ID/tables/+rep}
   */
  static String buildRepTablePath(final InstanceId iid) {
    return ZooUtil.getRoot(iid) + ZTABLES + "/" + REPLICATION_ID.canonical();
  }

  private void deleteReplicationTableZkEntries(ZooReaderWriter zrw, InstanceId iid) {
    String repTablePath = buildRepTablePath(iid);
    try {
      zrw.recursiveDelete(repTablePath, ZooUtil.NodeMissingPolicy.SKIP);
    } catch (KeeperException ex) {
      throw new IllegalStateException(
          "ZooKeeper error - failed recursive deletion on " + repTablePath, ex);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("interrupted deleting " + repTablePath + " from ZooKeeper",
          ex);
    }
  }

  private void deleteReplicationConfigs(ZooReaderWriter zrw, InstanceId iid, PropStore propStore) {
    List<PropStoreKey<?>> ids = getPropKeysFromZkIds(zrw, iid);
    for (PropStoreKey<?> storeKey : ids) {
      log.trace("Upgrade - remove replication iterators checking: {} ", storeKey);
      var p = propStore.get(storeKey);
      var props = p.asMap();
      List<String> filtered = filterReplConfigKeys(props.keySet());
      if (filtered.size() > 0) {
        log.debug("Upgrade filter replication iterators for: {}", storeKey);
        propStore.removeProperties(storeKey, filtered);
      }
    }
  }

  /**
   * Return a list of property keys that match replication iterator settings. This is specifically a
   * narrow filter to avoid potential matches with user define or properties that contain
   * replication in the property name (specifically table.file.replication which set hdfs block
   * replication.)
   */
  List<String> filterReplConfigKeys(Set<String> keys) {
    String REPL_ITERATOR_PATTERN = "^table\\.iterator\\.(majc|minc|scan)\\.replcombiner$";
    String REPL_COLUMN_PATTERN =
        "^table\\.iterator\\.(majc|minc|scan)\\.replcombiner\\.opt\\.columns$";

    Pattern p = Pattern.compile("(" + REPL_ITERATOR_PATTERN + "|" + REPL_COLUMN_PATTERN + ")");

    return keys.stream().filter(e -> p.matcher(e).find()).collect(Collectors.toList());
  }

  /**
   * Create a list of propStore keys reading the table ids directly from ZooKeeper.
   */
  private List<PropStoreKey<?>> getPropKeysFromZkIds(final ZooReaderWriter zrw,
      final InstanceId iid) {

    List<PropStoreKey<?>> result = new ArrayList<>();

    result.add(SystemPropKey.of(iid));

    // namespaces
    String nsRoot = ZooUtil.getRoot(iid) + ZNAMESPACES;
    try {
      List<String> c = zrw.getChildren(nsRoot);
      c.forEach(ns -> result.add(NamespacePropKey.of(iid, NamespaceId.of(ns))));
    } catch (KeeperException | InterruptedException ex) {
      throw new IllegalStateException("Failed to read namespace ids from " + nsRoot, ex);
    }

    // tables
    String tRoot = ZooUtil.getRoot(iid) + ZTABLES;
    try {
      List<String> c = zrw.getChildren(tRoot);
      c.forEach(t -> result.add(TablePropKey.of(iid, TableId.of(t))));
    } catch (KeeperException | InterruptedException ex) {
      throw new IllegalStateException("Failed to read table ids from " + nsRoot, ex);
    }

    return result;
  }
}
