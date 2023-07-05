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

import static org.apache.accumulo.core.metadata.schema.MetadataSchema.RESERVED_PREFIX;

import java.util.Map;

import org.apache.accumulo.core.client.admin.TabletHostingGoal;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.Ample.DataLevel;
import org.apache.accumulo.core.metadata.schema.Ample.TabletsMutator;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ExternalCompactionColumnFamily;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.schema.Section;
import org.apache.accumulo.server.ServerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class Upgrader11to12 implements Upgrader {

  private static final Logger LOG = LoggerFactory.getLogger(Upgrader11to12.class);

  @Override
  public void upgradeZookeeper(ServerContext context) {
    LOG.info("setting root table stored hosting goal");
    addHostingGoalToRootTable(context);
  }

  @Override
  public void upgradeRoot(ServerContext context) {
    LOG.info("setting metadata table hosting goal");
    addHostingGoalToMetadataTable(context);
  }

  @Override
  public void upgradeMetadata(ServerContext context) {
    LOG.info("setting hosting goal on user tables");
    addHostingGoalToUserTables(context);
    deleteExternalCompactionFinalStates(context);
    deleteExternalCompactions(context);
  }

  private void deleteExternalCompactionFinalStates(ServerContext context) {
    // This metadata was only written for user tablets as part of the compaction commit process.
    // Compactions are committed in a completely different way now, so delete these entries. Its
    // possible some completed compactions may need to be redone, but processing these entries would
    // not be easy to test so its better for correctness to delete them and redo the work.
    try (var scanner = context.createScanner(MetadataTable.NAME);
        var writer = context.createBatchWriter(MetadataTable.NAME)) {
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
      tm.forEach(
          t -> mut.mutateTablet(t.getExtent()).putHostingGoal(TabletHostingGoal.ALWAYS).mutate());
    }
  }

  private void addHostingGoalToRootTable(ServerContext context) {
    addHostingGoalToSystemTable(context, RootTable.ID);
  }

  private void addHostingGoalToMetadataTable(ServerContext context) {
    addHostingGoalToSystemTable(context, MetadataTable.ID);
  }

  private void addHostingGoalToUserTables(ServerContext context) {
    try (
        TabletsMetadata tm = context.getAmple().readTablets().forLevel(DataLevel.USER)
            .fetch(ColumnType.PREV_ROW).build();
        TabletsMutator mut = context.getAmple().mutateTablets()) {
      tm.forEach(
          t -> mut.mutateTablet(t.getExtent()).putHostingGoal(TabletHostingGoal.ONDEMAND).mutate());
    }
  }

  private void deleteExternalCompactions(ServerContext context) {
    // External compactions were only written for user tablets in 3.x and earlier, so only need to
    // process the metadata table. The metadata related to an external compaction has changed so
    // delete any that exists. Not using Ample in case there are problems deserializing the old
    // external compaction metadata.
    try (var scanner = context.createScanner(MetadataTable.NAME);
        var writer = context.createBatchWriter(MetadataTable.NAME)) {
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
}
