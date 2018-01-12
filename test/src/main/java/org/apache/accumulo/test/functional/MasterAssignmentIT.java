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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.FileNotFoundException;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.client.impl.Credentials;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.fate.util.UtilWaitThread;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.server.master.state.MetaDataTableScanner;
import org.apache.accumulo.server.master.state.TabletLocationState;
import org.apache.commons.configuration.ConfigurationException;
import org.junit.Test;

public class MasterAssignmentIT extends AccumuloClusterHarness {

  @Override
  protected int defaultTimeoutSeconds() {
    return 2 * 60;
  }

  @Test
  public void test() throws Exception {
    Connector c = getConnector();
    String tableName = super.getUniqueNames(1)[0];
    c.tableOperations().create(tableName);
    String tableId = c.tableOperations().tableIdMap().get(tableName);
    // wait for the table to be online
    TabletLocationState newTablet;
    do {
      UtilWaitThread.sleep(250);
      newTablet = getTabletLocationState(c, tableId);
    } while (newTablet.current == null);
    assertNull(newTablet.last);
    assertNull(newTablet.future);

    // put something in it
    BatchWriter bw = c.createBatchWriter(tableName, new BatchWriterConfig());
    Mutation m = new Mutation("a");
    m.put("b", "c", "d");
    bw.addMutation(m);
    bw.close();
    // give it a last location
    c.tableOperations().flush(tableName, null, null, true);

    TabletLocationState flushed = getTabletLocationState(c, tableId);
    assertEquals(newTablet.current, flushed.current);
    assertEquals(flushed.current, flushed.last);
    assertNull(newTablet.future);

    // take the tablet offline
    c.tableOperations().offline(tableName, true);
    TabletLocationState offline = getTabletLocationState(c, tableId);
    assertNull(offline.future);
    assertNull(offline.current);
    assertEquals(flushed.current, offline.last);

    // put it back online
    c.tableOperations().online(tableName, true);
    TabletLocationState online = getTabletLocationState(c, tableId);
    assertNull(online.future);
    assertNotNull(online.current);
    assertEquals(online.current, online.last);
  }

  private TabletLocationState getTabletLocationState(Connector c, String tableId) throws FileNotFoundException, ConfigurationException {
    Credentials creds = new Credentials(getAdminPrincipal(), getAdminToken());
    ClientContext context = new ClientContext(c.getInstance(), creds, getCluster().getClientConfig());
    try (MetaDataTableScanner s = new MetaDataTableScanner(context, new Range(KeyExtent.getMetadataEntry(Table.ID.of(tableId), null)))) {
      TabletLocationState tlState = s.next();
      return tlState;
    }
  }
}
