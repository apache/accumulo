/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.test.functional;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.Tables;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.VerifyIngest.VerifyParams;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class TrashCanIT extends AccumuloClusterHarness {

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(Property.GENERAL_TRASH_ENABLED, "true");
    cfg.setProperty(Property.GENERAL_TRASH_TIMER_PERIOD, "1s");
    cfg.setProperty(Property.GENERAL_TRASH_PERIOD, "2s");
  }

  @Override
  protected int defaultTimeoutSeconds() {
    return 50 * 60;
  }

  @Test
  public void deleteWithTrashTest() throws Exception {
    String[] tableNames = getUniqueNames(1);
    String name = tableNames[0];
    VerifyParams params = new VerifyParams(cluster.getClientProperties(), name);
    params.createTable = true;

    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      TestIngest.ingest(c, params);
      TableId tableId = Tables.getTableId((ClientContext) c, name);
      TableState tabeleState = Tables.getTableState((ClientContext) c, tableId);
      String tableName = Tables.getTableName((ClientContext) c, tableId);

      c.tableOperations().delete(name);

      tabeleState = Tables.getTableState((ClientContext) c, tableId);
      assertTrue(TableState.TRASH.equals(tabeleState));

      String trashName = Tables.getTrashTableName((ClientContext) c, tableId);
      c.tableOperations().delete(trashName);

      assertFalse(Tables.exists((ClientContext) c, tableId));
      tabeleState = Tables.getTableState((ClientContext) c, tableId);
      assertTrue(TableState.UNKNOWN.equals(tabeleState));
    }
  }

  @Test
  public void undeleteFromTrashTest() throws Exception {
    String[] tableNames = getUniqueNames(1);
    String name = tableNames[0];
    VerifyParams params = new VerifyParams(cluster.getClientProperties(), name);
    params.createTable = true;

    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      TestIngest.ingest(c, params);
      TableId tableId = Tables.getTableId((ClientContext) c, name);
      TableState tabeleState = Tables.getTableState((ClientContext) c, tableId);
      String tableName = Tables.getTableName((ClientContext) c, tableId);

      c.tableOperations().delete(name);

      tabeleState = Tables.getTableState((ClientContext) c, tableId);
      assertTrue(TableState.TRASH.equals(tabeleState));

      String trashName = Tables.getTrashTableName((ClientContext) c, tableId);
      c.tableOperations().undelete(trashName);

      assertTrue(Tables.exists((ClientContext) c, tableId));
      tabeleState = Tables.getTableState((ClientContext) c, tableId);
      assertTrue(TableState.ONLINE.equals(tabeleState));

      String newTableName = Tables.getTableName((ClientContext) c, tableId);
      assertTrue(tableName.equals(newTableName));
    }
  }

  @Test
  public void timerEmptyTrashTest() throws Exception {
    String[] tableNames = getUniqueNames(1);
    String name = tableNames[0];
    VerifyParams params = new VerifyParams(cluster.getClientProperties(), name);
    params.createTable = true;

    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      TestIngest.ingest(c, params);
      TableId tableId = Tables.getTableId((ClientContext) c, name);
      TableState tabeleState = Tables.getTableState((ClientContext) c, tableId);
      String tableName = Tables.getTableName((ClientContext) c, tableId);

      c.tableOperations().delete(name);

      tabeleState = Tables.getTableState((ClientContext) c, tableId);
      assertTrue(TableState.TRASH.equals(tabeleState));

      // to make sure the task is triggered
      Thread.sleep(3000l);

      assertFalse(Tables.exists((ClientContext) c, tableId));
    }
  }

}
