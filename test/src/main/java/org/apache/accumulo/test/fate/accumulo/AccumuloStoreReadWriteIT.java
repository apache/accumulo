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
package org.apache.accumulo.test.fate.accumulo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.Fate.TxInfo;
import org.apache.accumulo.core.fate.FateStore.FateTxStore;
import org.apache.accumulo.core.fate.ReadOnlyFateStore.TStatus;
import org.apache.accumulo.core.fate.ReadOnlyRepo;
import org.apache.accumulo.core.fate.accumulo.AccumuloStore;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.test.fate.zookeeper.FateIT.TestOperation;
import org.junit.jupiter.api.Test;

public class AccumuloStoreReadWriteIT extends AccumuloClusterHarness {

  private static final NamespaceId NS = NamespaceId.of("testNameSpace");
  private static final TableId TID = TableId.of("testTable");

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(1);
  }

  @Test
  public void testReadWrite() throws Exception {
    final String table = getUniqueNames(1)[0];
    try (ClientContext client =
        (ClientContext) Accumulo.newClient().from(getClientProps()).build()) {
      client.tableOperations().create(table);

      AccumuloStore<Manager> store = new AccumuloStore<>(client, table);
      // Verify no transactions
      assertEquals(0, store.list().size());

      // Create a new transaction and get the store for it
      long tid = store.create();
      FateTxStore<Manager> txStore = store.reserve(tid);
      assertTrue(txStore.timeCreated() > 0);
      assertEquals(1, store.list().size());

      // Push a test FATE op and verify we can read it back
      txStore.push(new TestOperation(NS, TID));
      TestOperation op = (TestOperation) txStore.top();
      assertNotNull(op);

      // Test status
      txStore.setStatus(TStatus.SUBMITTED);
      assertEquals(TStatus.SUBMITTED, txStore.getStatus());

      // Set a name to test setTransactionInfo()
      txStore.setTransactionInfo(TxInfo.TX_NAME, "name");
      assertEquals("name", txStore.getTransactionInfo(TxInfo.TX_NAME));

      // Try setting a second test op to test getStack()
      // when listing or popping TestOperation2 should be first
      assertEquals(1, txStore.getStack().size());
      txStore.push(new TestOperation2(NS, TID));
      // test top returns TestOperation2
      ReadOnlyRepo<Manager> top = txStore.top();
      assertInstanceOf(TestOperation2.class, top);

      // test get stack
      List<ReadOnlyRepo<Manager>> ops = txStore.getStack();
      assertEquals(2, ops.size());
      assertInstanceOf(TestOperation2.class, ops.get(0));
      assertEquals(TestOperation.class, ops.get(1).getClass());

      // test pop, TestOperation should be left
      txStore.pop();
      ops = txStore.getStack();
      assertEquals(1, ops.size());
      assertEquals(TestOperation.class, ops.get(0).getClass());

      // create second
      FateTxStore<Manager> txStore2 = store.reserve(store.create());
      assertEquals(2, store.list().size());

      // test delete
      txStore.delete();
      assertEquals(1, store.list().size());
      txStore2.delete();
      assertEquals(0, store.list().size());
    }
  }

  private static class TestOperation2 extends TestOperation {

    public TestOperation2(NamespaceId namespaceId, TableId tableId) {
      super(namespaceId, tableId);
    }
  }

}
