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

import org.apache.accumulo.core.client.admin.TabletHostingGoal;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.Ample.DataLevel;
import org.apache.accumulo.core.metadata.schema.Ample.TabletsMutator;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.server.ServerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  }

  private void addHostingGoalToSystemTable(ServerContext context, TableId tableId) {
    try (
        TabletsMetadata tm =
            context.getAmple().readTablets().forTable(tableId).fetch(ColumnType.PREV_ROW).build();
        TabletsMutator mut = context.getAmple().mutateTablets()) {
      tm.forEach(t -> mut.mutateTablet(t.getExtent()).setHostingGoal(TabletHostingGoal.ALWAYS));
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
      tm.forEach(t -> mut.mutateTablet(t.getExtent()).setHostingGoal(TabletHostingGoal.ONDEMAND));
    }
  }

}
