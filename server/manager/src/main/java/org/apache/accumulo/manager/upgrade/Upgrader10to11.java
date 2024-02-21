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
import static org.apache.accumulo.core.Constants.ZTABLES;
import static org.apache.accumulo.core.Constants.ZTABLE_STATE;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.RESERVED_PREFIX;
import static org.apache.accumulo.server.util.MetadataTableUtil.EMPTY_TEXT;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.core.metadata.AccumuloTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.store.PropStore;
import org.apache.accumulo.server.conf.store.PropStoreKey;
import org.apache.accumulo.server.conf.store.TablePropKey;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Upgrader10to11 implements Upgrader {

  private static final Logger log = LoggerFactory.getLogger(Upgrader10to11.class);

  // Included for upgrade code usage any other usage post 3.0 should not be used.
  private static final TableId REPLICATION_ID = TableId.of("+rep");

  private static final Range REP_TABLE_RANGE =
      new Range(REPLICATION_ID.canonical() + ";", true, REPLICATION_ID.canonical() + "<", true);

  // copied from MetadataSchema 2.1 (removed in 3.0)
  private static final Range REP_WAL_RANGE =
      new Range(RESERVED_PREFIX + "repl", true, RESERVED_PREFIX + "repm", false);

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

    cleanMetaConfig(iid, context.getPropStore());

    deleteReplicationTableZkEntries(zrw, iid);

  }

  @Override
  public void upgradeRoot(final ServerContext context) {
    log.info("upgrade root - skipping, nothing to do");
  }

  @Override
  public void upgradeMetadata(final ServerContext context) {
    log.info("upgrade metadata entries");
    List<String> replTableFiles = readReplFilesFromMetadata(context);
    deleteReplMetadataEntries(context);
    deleteReplTableFiles(context, replTableFiles);
  }

  List<String> readReplFilesFromMetadata(final ServerContext context) {
    List<String> results = new ArrayList<>();
    try (Scanner scanner =
        context.createScanner(AccumuloTable.METADATA.tableName(), Authorizations.EMPTY)) {
      scanner.fetchColumnFamily(MetadataSchema.TabletsSection.DataFileColumnFamily.NAME);
      scanner.setRange(REP_TABLE_RANGE);
      for (Map.Entry<Key,Value> entry : scanner) {
        String f = entry.getKey()
            .getColumnQualifier(MetadataSchema.TabletsSection.DataFileColumnFamily.NAME).toString();
        results.add(f);
      }
    } catch (TableNotFoundException ex) {
      throw new IllegalStateException("failed to read replication files from metadata", ex);
    }
    return results;
  }

  void deleteReplTableFiles(final ServerContext context, final List<String> replTableFiles) {
    // short circuit if there are no files
    if (replTableFiles.isEmpty()) {
      return;
    }
    // write delete mutations
    boolean haveFailures = false;
    try (BatchWriter writer = context.createBatchWriter(AccumuloTable.METADATA.tableName())) {
      for (String filename : replTableFiles) {
        Mutation m = createDelMutation(filename);
        log.debug("Adding delete marker for file: {}", filename);
        writer.addMutation(m);
      }
    } catch (MutationsRejectedException ex) {
      log.debug("Failed to write delete marker {}", ex.getMessage());
      haveFailures = true;
    } catch (TableNotFoundException ex) {
      throw new IllegalStateException("failed to read replication files from metadata", ex);
    }
    if (haveFailures) {
      throw new IllegalStateException(
          "deletes rejected adding deletion marker for replication file entries, check log");
    }
  }

  private Mutation createDelMutation(String path) {
    Mutation delFlag = new Mutation(new Text(MetadataSchema.DeletesSection.encodeRow(path)));
    delFlag.put(EMPTY_TEXT, EMPTY_TEXT, MetadataSchema.DeletesSection.SkewedKeyValue.NAME);
    return delFlag;
  }

  /**
   * remove +rep entries from metadata.
   */
  private void deleteReplMetadataEntries(final ServerContext context) {
    try (BatchDeleter deleter =
        context.createBatchDeleter(AccumuloTable.METADATA.tableName(), Authorizations.EMPTY, 10)) {
      deleter.setRanges(List.of(REP_TABLE_RANGE, REP_WAL_RANGE));
      deleter.delete();
    } catch (TableNotFoundException | MutationsRejectedException ex) {
      throw new IllegalStateException("failed to remove replication info from metadata table", ex);
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

  private void cleanMetaConfig(final InstanceId iid, final PropStore propStore) {
    PropStoreKey<TableId> metaKey = TablePropKey.of(iid, AccumuloTable.METADATA.tableId());
    var p = propStore.get(metaKey);
    var props = p.asMap();
    List<String> filtered = filterReplConfigKeys(props.keySet());
    // add replication status formatter to remove list.
    String v = props.get("table.formatter");
    if (v != null && v.compareTo("org.apache.accumulo.server.replication.StatusFormatter") == 0) {
      filtered.add("table.formatter");
    }

    if (filtered.size() > 0) {
      log.trace("Upgrade filtering replication iterators for id: {}", metaKey);
      propStore.removeProperties(metaKey, filtered);
    }
  }

  /**
   * Return a list of property keys that match replication iterator settings. This is specifically a
   * narrow filter to avoid potential matches with user define or properties that contain
   * replication in the property name (specifically table.file.replication which set hdfs block
   * replication.)
   */
  private List<String> filterReplConfigKeys(Set<String> keys) {
    String REPL_ITERATOR_PATTERN = "^table\\.iterator\\.(majc|minc|scan)\\.replcombiner$";
    String REPL_COLUMN_PATTERN =
        "^table\\.iterator\\.(majc|minc|scan)\\.replcombiner\\.opt\\.columns$";

    Pattern p = Pattern.compile("(" + REPL_ITERATOR_PATTERN + "|" + REPL_COLUMN_PATTERN + ")");

    return keys.stream().filter(e -> p.matcher(e).find()).collect(Collectors.toList());
  }
}
