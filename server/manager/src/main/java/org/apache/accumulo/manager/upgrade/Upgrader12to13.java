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
import static org.apache.accumulo.core.metadata.RootTable.ZROOT_TABLET;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.RESERVED_PREFIX;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.Upgrade12to13.COMPACT_COL;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.admin.TabletAvailability;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.core.metadata.AccumuloTable;
import org.apache.accumulo.core.metadata.schema.Ample.DataLevel;
import org.apache.accumulo.core.metadata.schema.Ample.TabletsMutator;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ExternalCompactionColumnFamily;
import org.apache.accumulo.core.metadata.schema.RootTabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.schema.Section;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.store.TablePropKey;
import org.apache.accumulo.server.util.PropUtil;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

//TODO when removing this class, also remove MetadataSchema.Upgrader12to13
public class Upgrader12to13 implements Upgrader {

  private static final Logger LOG = LoggerFactory.getLogger(Upgrader12to13.class);

  @Override
  public void upgradeZookeeper(ServerContext context) {
    LOG.info("setting root table stored hosting availability");
    addHostingGoalToRootTable(context);
    LOG.info("Removing compact-id paths from ZooKeeper");
    removeZKCompactIdPaths(context);
    LOG.info("Removing compact columns from root tablet");
    removeCompactColumnsFromRootTabletMetadata(context);
  }

  @Override
  public void upgradeRoot(ServerContext context) {
    LOG.info("Looking for partial splits");
    handlePartialSplits(context, AccumuloTable.ROOT.tableName());
    LOG.info("setting metadata table hosting availability");
    addHostingGoalToMetadataTable(context);
    LOG.info("Removing MetadataBulkLoadFilter iterator from root table");
    removeMetaDataBulkLoadFilter(context, AccumuloTable.ROOT.tableId());
    LOG.info("Removing compact columns from metadata tablets");
    removeCompactColumnsFromTable(context, AccumuloTable.ROOT.tableName());
  }

  @Override
  public void upgradeMetadata(ServerContext context) {
    LOG.info("Looking for partial splits");
    handlePartialSplits(context, AccumuloTable.METADATA.tableName());
    LOG.info("setting hosting availability on user tables");
    addHostingGoalToUserTables(context);
    LOG.info("Deleting external compaction final states from user tables");
    deleteExternalCompactionFinalStates(context);
    LOG.info("Deleting external compaction from user tables");
    deleteExternalCompactions(context);
    LOG.info("Removing MetadataBulkLoadFilter iterator from metadata table");
    removeMetaDataBulkLoadFilter(context, AccumuloTable.METADATA.tableId());
    LOG.info("Removing compact columns from user tables");
    removeCompactColumnsFromTable(context, AccumuloTable.METADATA.tableName());
  }

  private void removeCompactColumnsFromRootTabletMetadata(ServerContext context) {
    var rootBase = ZooUtil.getRoot(context.getInstanceID()) + ZROOT_TABLET;

    try {
      var zrw = context.getZooReaderWriter();
      Stat stat = new Stat();
      byte[] rootData = zrw.getData(rootBase, stat);

      String json = new String(rootData, UTF_8);

      var rtm = new RootTabletMetadata(json);

      ArrayList<Mutation> mutations = new ArrayList<>();
      for (Map.Entry<Key,Value> entry : rtm.toKeyValues().entrySet()) {
        var key = entry.getKey();

        if (COMPACT_COL.hasColumns(key)) {
          var row = key.getRow();
          Preconditions.checkState(key.getColumnVisibilityData().length() == 0,
              "Expected empty visibility, saw %s ", key.getColumnVisibilityData());
          Mutation m = new Mutation(row);
          // TODO will metadata contraint fail when this is written?
          COMPACT_COL.putDelete(m);
          mutations.add(m);
        }
      }

      Preconditions.checkState(mutations.size() <= 1);

      if (!mutations.isEmpty()) {
        LOG.info("Root metadata in ZooKeeper before upgrade: {}", json);
        rtm.update(mutations.get(0));
        zrw.overwritePersistentData(rootBase, rtm.toJson().getBytes(UTF_8), stat.getVersion());
        LOG.info("Root metadata in ZooKeeper after upgrade: {}", rtm.toJson());
      }
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(
          "Could not read root metadata from ZooKeeper due to interrupt", ex);
    } catch (KeeperException ex) {
      throw new IllegalStateException(
          "Could not read or write root metadata in ZooKeeper because of ZooKeeper exception", ex);
    }

  }

  private void removeCompactColumnsFromTable(ServerContext context, String tableName) {

    try (var scanner = context.createScanner(tableName);
        var writer = context.createBatchWriter(tableName)) {
      scanner.setRange(MetadataSchema.TabletsSection.getRange());
      COMPACT_COL.fetch(scanner);

      for (Map.Entry<Key,Value> entry : scanner) {
        var key = entry.getKey();
        if (COMPACT_COL.hasColumns(key)) {
          var row = key.getRow();
          Preconditions.checkState(key.getColumnVisibilityData().length() == 0,
              "Expected empty visibility, saw %s ", key.getColumnVisibilityData());
          Mutation m = new Mutation(row);
          COMPACT_COL.putDelete(m);
          writer.addMutation(m);
        }
      }
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private void removeZKCompactIdPaths(ServerContext context) {
    final String ZTABLE_COMPACT_ID = "/compact-id";
    final String ZTABLE_COMPACT_CANCEL_ID = "/compact-cancel-id";

    for (Entry<String,String> e : context.tableOperations().tableIdMap().entrySet()) {
      final String tName = e.getKey();
      final String tId = e.getValue();
      final String zTablePath = Constants.ZROOT + "/" + context.getInstanceID().canonical()
          + Constants.ZTABLES + "/" + tId;
      try {
        context.getZooReaderWriter().delete(zTablePath + ZTABLE_COMPACT_ID);
        context.getZooReaderWriter().delete(zTablePath + ZTABLE_COMPACT_CANCEL_ID);
      } catch (KeeperException | InterruptedException e1) {
        throw new IllegalStateException(
            "Error removing compaction ids from ZooKeeper for table: " + tName);
      }
    }
  }

  private void removeMetaDataBulkLoadFilter(ServerContext context, TableId tableId) {
    final String propName = Property.TABLE_ITERATOR_PREFIX.getKey() + "majc.bulkLoadFilter";
    PropUtil.removeProperties(context, TablePropKey.of(context, tableId), List.of(propName));
  }

  private void deleteExternalCompactionFinalStates(ServerContext context) {
    // This metadata was only written for user tablets as part of the compaction commit process.
    // Compactions are committed in a completely different way now, so delete these entries. Its
    // possible some completed compactions may need to be redone, but processing these entries would
    // not be easy to test so its better for correctness to delete them and redo the work.
    try (var scanner = context.createScanner(AccumuloTable.METADATA.tableName());
        var writer = context.createBatchWriter(AccumuloTable.METADATA.tableName())) {
      var section = new Section(RESERVED_PREFIX + "ecomp", true, RESERVED_PREFIX + "ecomq", false);
      scanner.setRange(section.getRange());

      for (Map.Entry<Key,Value> entry : scanner) {
        var key = entry.getKey();
        var row = key.getRow();
        Preconditions.checkState(row.toString().startsWith(section.getRowPrefix()));
        Mutation m = new Mutation(row);
        Preconditions.checkState(key.getColumnVisibilityData().length() == 0,
            "Expected empty visibility, saw %s ", key.getColumnVisibilityData());
        m.putDelete(key.getColumnFamily(), key.getColumnQualifier());
        writer.addMutation(m);
      }
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private void addHostingGoalToSystemTable(ServerContext context, TableId tableId) {
    try (
        TabletsMetadata tm =
            context.getAmple().readTablets().forTable(tableId).fetch(ColumnType.PREV_ROW).build();
        TabletsMutator mut = context.getAmple().mutateTablets()) {
      tm.forEach(t -> mut.mutateTablet(t.getExtent())
          .putTabletAvailability(TabletAvailability.HOSTED).mutate());
    }
  }

  private void addHostingGoalToRootTable(ServerContext context) {
    addHostingGoalToSystemTable(context, AccumuloTable.ROOT.tableId());
  }

  private void addHostingGoalToMetadataTable(ServerContext context) {
    addHostingGoalToSystemTable(context, AccumuloTable.METADATA.tableId());
  }

  private void addHostingGoalToUserTables(ServerContext context) {
    try (
        TabletsMetadata tm = context.getAmple().readTablets().forLevel(DataLevel.USER)
            .fetch(ColumnType.PREV_ROW).build();
        TabletsMutator mut = context.getAmple().mutateTablets()) {
      tm.forEach(t -> mut.mutateTablet(t.getExtent())
          .putTabletAvailability(TabletAvailability.ONDEMAND).mutate());
    }
  }

  private void deleteExternalCompactions(ServerContext context) {
    // External compactions were only written for user tablets in 3.x and earlier, so only need to
    // process the metadata table. The metadata related to an external compaction has changed so
    // delete any that exists. Not using Ample in case there are problems deserializing the old
    // external compaction metadata.
    try (var scanner = context.createScanner(AccumuloTable.METADATA.tableName());
        var writer = context.createBatchWriter(AccumuloTable.METADATA.tableName())) {
      scanner.setRange(TabletsSection.getRange());
      scanner.fetchColumnFamily(ExternalCompactionColumnFamily.NAME);

      for (Map.Entry<Key,Value> entry : scanner) {
        var key = entry.getKey();
        Mutation m = new Mutation(key.getRow());
        Preconditions.checkState(key.getColumnFamily().equals(ExternalCompactionColumnFamily.NAME),
            "Expected family %s, saw %s ", ExternalCompactionColumnFamily.NAME,
            key.getColumnVisibilityData());
        Preconditions.checkState(key.getColumnVisibilityData().length() == 0,
            "Expected empty visibility, saw %s ", key.getColumnVisibilityData());
        m.putDelete(key.getColumnFamily(), key.getColumnQualifier());
        writer.addMutation(m);
      }
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private void handlePartialSplits(ServerContext context, String table) {
    try (var scanner = context.createScanner(table, Authorizations.EMPTY)) {
      scanner.setRange(TabletsSection.getRange());
      TabletsSection.Upgrade12to13.SPLIT_RATIO_COLUMN.fetch(scanner);

      for (var entry : scanner) {
        SplitRecovery12to13.fixSplit(context, entry.getKey().getRow());
      }
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }
}
