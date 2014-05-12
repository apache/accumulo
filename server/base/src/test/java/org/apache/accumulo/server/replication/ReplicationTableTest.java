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
package org.apache.accumulo.server.replication;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import com.google.common.collect.ImmutableMap;

/**
 * 
 */
public class ReplicationTableTest {

  @Rule
  public TestName testName = new TestName();

  private MockInstance instance;
  private Connector conn;
  
  @Before
  public void setupMockAccumulo() throws Exception {
    instance = new MockInstance(testName.getMethodName());
    conn = instance.getConnector("root", new PasswordToken(""));
  }

  @Test
  public void replicationTableCreated() {
    TableOperations tops = conn.tableOperations();
    Assert.assertFalse(tops.exists(ReplicationTable.NAME));
    ReplicationTable.create(conn);
    Assert.assertTrue(tops.exists(ReplicationTable.NAME));
  }

  @Test
  public void replicationTableDoubleCreate() {
    TableOperations tops = conn.tableOperations();
    Assert.assertFalse(tops.exists(ReplicationTable.NAME));

    // Create the table
    ReplicationTable.create(conn);

    // Make sure it exists and save off the id
    Assert.assertTrue(tops.exists(ReplicationTable.NAME));
    String tableId = tops.tableIdMap().get(ReplicationTable.NAME);

    // Try to make it again, should return quickly
    ReplicationTable.create(conn);
    Assert.assertTrue(tops.exists(ReplicationTable.NAME));

    // Verify we have the same table as previously
    Assert.assertEquals(tableId, tops.tableIdMap().get(ReplicationTable.NAME));
  }

  @Test
  public void configureOnExistingTable() throws Exception {
    TableOperations tops = conn.tableOperations();

    // Create the table by hand
    tops.create(ReplicationTable.NAME);
    Map<String,EnumSet<IteratorScope>> iterators = tops.listIterators(ReplicationTable.NAME);

    Assert.assertFalse(iterators.containsKey(ReplicationTable.COMBINER_NAME));

    ReplicationTable.configureReplicationTable(conn);

    // After configure the iterator should be set
    iterators = tops.listIterators(ReplicationTable.NAME);
    Assert.assertTrue(iterators.containsKey(ReplicationTable.COMBINER_NAME));

    // Needs to be set below versioning
    IteratorSetting expected = new IteratorSetting(30, ReplicationTable.COMBINER_NAME, StatusCombiner.class);
    Combiner.setCombineAllColumns(expected, true);
    IteratorSetting setting = tops.getIteratorSetting(ReplicationTable.NAME, ReplicationTable.COMBINER_NAME, IteratorScope.scan);

    Assert.assertEquals(expected, setting);

    // Check for locality groups too
    Map<String,Set<Text>> expectedLocalityGroups = ImmutableMap.of(ReplicationTable.STATUS_LG_NAME, ReplicationTable.STATUS_LG_COLFAMS,
        ReplicationTable.WORK_LG_NAME, ReplicationTable.WORK_LG_COLFAMS);
    Assert.assertEquals(expectedLocalityGroups, tops.getLocalityGroups(ReplicationTable.NAME));
  }

  @Test
  public void disablesVersioning() throws Exception {
    TableOperations tops = conn.tableOperations();

    if (tops.exists(ReplicationTable.NAME)) {
      tops.delete(ReplicationTable.NAME);
    }

    ReplicationTable.create(conn);

    Set<String> iters = tops.listIterators(ReplicationTable.NAME).keySet();

    Assert.assertEquals(Collections.singleton(ReplicationTable.COMBINER_NAME), iters);
  }
}
