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
package org.apache.accumulo.test.ample;

import static org.apache.accumulo.core.client.ConditionalWriter.Status.ACCEPTED;
import static org.apache.accumulo.core.client.ConditionalWriter.Status.UNKNOWN;
import static org.apache.accumulo.test.ample.ConditionalWriterInterceptor.withStatus;
import static org.apache.accumulo.test.ample.TestAmple.mockWithAmple;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.FateInstanceType;
import org.apache.accumulo.core.metadata.schema.Ample.ConditionalResult.Status;
import org.apache.accumulo.core.metadata.schema.Ample.DataLevel;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.SplitColumnFamily;
import org.apache.accumulo.core.metadata.schema.TabletOperationId;
import org.apache.accumulo.core.metadata.schema.TabletOperationType;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.split.FindSplits;
import org.apache.accumulo.manager.tableOps.split.PreSplit;
import org.apache.accumulo.test.ample.TestAmple.TestServerAmpleImpl;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.google.common.base.Preconditions;

public class TestAmpleIT extends SharedMiniClusterBase {

  @BeforeAll
  public static void setup() throws Exception {
    SharedMiniClusterBase.startMiniCluster();
  }

  @AfterAll
  public static void teardown() {
    SharedMiniClusterBase.stopMiniCluster();
  }

  @Test
  public void testCreateMetadataFromExisting() throws Exception {

    String[] tableNames = getUniqueNames(2);
    String metadataTable = tableNames[0];
    String userTable = tableNames[1];

    try (ClientContext client =
        (ClientContext) Accumulo.newClient().from(getClientProps()).build()) {
      client.tableOperations().create(userTable);

      TestAmple.createMetadataTable(client, metadataTable);

      TableId tableId = TableId.of(client.tableOperations().tableIdMap().get(userTable));

      TestServerAmpleImpl ample = (TestServerAmpleImpl) TestAmple
          .create(getCluster().getServerContext(), Map.of(DataLevel.USER, metadataTable));

      ample.createMetadataFromExisting(client, tableId);

      var count = new AtomicInteger();
      try (var tablets = ample.readTablets().forTable(tableId).build().stream()) {
        tablets.forEach(tm -> {
          assertNotNull(tm.getTableId());
          assertNotNull(tm.getExtent());
          assertNotNull(tm.getTabletAvailability());
          assertNotNull(tm.getTime());
          count.incrementAndGet();
        });
      }
      assertEquals(1, count.get());
    }
  }

  @Test
  public void testCreateMetadata() throws Exception {
    // final var ctx = getCluster().getServerContext();
    String[] tableNames = getUniqueNames(2);
    String metadataTable = tableNames[0];
    String userTable = tableNames[1];

    try (ClientContext client =
        (ClientContext) Accumulo.newClient().from(getClientProps()).build()) {
      client.tableOperations().create(userTable);
      TestAmple.createMetadataTable(client, metadataTable);

      TableId tableId = TableId.of("1");
      TestServerAmpleImpl ample = (TestServerAmpleImpl) TestAmple
          .create(getCluster().getServerContext(), Map.of(DataLevel.USER, metadataTable));

      ample.createMetadata(tableId);

      var count = new AtomicInteger();
      try (var tablets = ample.readTablets().forTable(tableId).build().stream()) {
        tablets.forEach(tm -> {
          assertNotNull(tm.getTableId());
          assertNotNull(tm.getExtent());
          assertNotNull(tm.getTabletAvailability());
          assertNotNull(tm.getTime());
          count.incrementAndGet();
        });
      }
      assertEquals(1, count.get());
    }

  }

  // This is an example test showing how to test a conditional
  // mutation rejection handler by using a ConditionalWriterInterceptor
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testUnknownStatus(boolean accepted) throws Exception {

    String[] tableNames = getUniqueNames(2);
    String metadataTable = tableNames[0] + accepted;
    String userTable = tableNames[1] + accepted;

    try (ClientContext client =
        (ClientContext) Accumulo.newClient().from(getClientProps()).build()) {
      client.tableOperations().create(userTable);

      TestAmple.createMetadataTable(client, metadataTable);
      TableId tableId = TableId.of(client.tableOperations().tableIdMap().get(userTable));

      TestServerAmpleImpl ample = (TestServerAmpleImpl) TestAmple
          .create(getCluster().getServerContext(), Map.of(DataLevel.USER, metadataTable));
      ample.createMetadataFromExisting(client, tableId);

      // Add a custom interceptor that will replace the result status with UNKNOWN
      // for only the first time the method is called instead of the actual state
      // (which should be accepted)
      //
      // When the result of UNKNOWN is returned, the mutator will trigger a retry
      // and resubmit the mutation. On retry, the mutation should be rejected because
      // the mutation requires an absent operation that will have already been set on
      // the previous submission.
      //
      // This will cause the mutator to check the rejection handler to see if we should actually
      // accept the mutation. This test uses a boolean to run twice to test both the
      // case of when the rejection handle will return true and false so that we can test that
      // the state is correctly set to either ACCEPTED or REJECTED after the rejection handler is
      // executed.
      try (
          var tabletsMutator = ample.conditionallyMutateTablets(withStatus(ACCEPTED, UNKNOWN, 1))) {
        var mutator = tabletsMutator.mutateTablet(new KeyExtent(tableId, null, null))
            .requireAbsentOperation();
        var fateId = FateId.from(FateInstanceType.USER, UUID.randomUUID());
        var opid = TabletOperationId.from(TabletOperationType.SPLITTING, fateId);

        mutator.putOperation(TabletOperationId.from(TabletOperationType.SPLITTING, fateId));

        // If accepted is true then we want to test the ACCEPTED case so return true if
        // the opid matches. If false we want this to fail to test REJECTED, so we return
        // false no matter what
        mutator.submit(afterMeta -> accepted && opid.equals(afterMeta.getOperationId()));

        var results = tabletsMutator.process();
        results.values().forEach(result -> {
          var status = result.getStatus();
          // check the state is correct
          Preconditions.checkState(accepted ? status == Status.ACCEPTED : status == Status.REJECTED,
              "Failed %s, %s", status, result.getExtent());
        });
      }
    }
  }

  // TODO: disable until fixed
  @Disabled
  @Test
  public void testFindSplits() throws Exception {

    String[] tableNames = getUniqueNames(2);
    String metadataTable = tableNames[0];
    String userTable = tableNames[1];

    try (ClientContext client =
        (ClientContext) Accumulo.newClient().from(getClientProps()).build()) {
      TestAmple.createMetadataTable(client, metadataTable);

      // Create table with a smaller max end row size
      createUnsplittableTable(client, userTable);
      populateUnsplittableTable(client, userTable);

      TableId tableId = TableId.of(client.tableOperations().tableIdMap().get(userTable));

      TestServerAmpleImpl ample = (TestServerAmpleImpl) TestAmple
          .create(getCluster().getServerContext(), Map.of(DataLevel.USER, metadataTable));
      ample.createMetadataFromExisting(client, tableId,
          Set.of(SplitColumnFamily.UNSPLITTABLE_COLUMN));

      KeyExtent extent = new KeyExtent(tableId, null, null);
      Manager manager = mockWithAmple(getCluster(), ample);

      // TODO: Need to fix this, the test ample is reading the real metadata table inside
      // findSplits call and finding an unsplittable column and not reading the test table,
      // need to figure out why the test impl is not using the test table correctly
      FindSplits findSplits = new FindSplits(extent);
      PreSplit preSplit = (PreSplit) findSplits
          .call(FateId.from(FateInstanceType.USER, UUID.randomUUID()), manager);

      // The table should not need splitting
      assertNull(preSplit);

      // Verify metadata has unsplittable column
      var count = new AtomicInteger();
      try (var tablets = ample.readTablets().forTable(tableId).build().stream()) {
        tablets.forEach(tm -> {
          assertNotNull(tm.getUnSplittable());
          count.incrementAndGet();
        });
      }
      assertEquals(1, count.get());
    }
  }

  private void createUnsplittableTable(ClientContext client, String table) throws Exception {
    // make a table and lower the configuration properties
    // @formatter:off
    Map<String,String> props = Map.of(
        Property.TABLE_SPLIT_THRESHOLD.getKey(), "1K",
        Property.TABLE_FILE_COMPRESSION_TYPE.getKey(), "none",
        Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE.getKey(), "64",
        Property.TABLE_MAX_END_ROW_SIZE.getKey(), "" + 100,
        Property.TABLE_MAJC_RATIO.getKey(), "9999"
    );
    // @formatter:on
    client.tableOperations().create(table, new NewTableConfiguration().setProperties(props));

  }

  private void populateUnsplittableTable(ClientContext client, String table) throws Exception {
    byte[] data = new byte[101];
    Arrays.fill(data, 0, data.length - 2, (byte) 'm');

    final int numOfMutations = 20;
    try (BatchWriter batchWriter = client.createBatchWriter(table)) {
      // Make the last place in the key different for every entry added to the table
      for (int i = 0; i < numOfMutations; i++) {
        data[data.length - 1] = (byte) i;
        Mutation m = new Mutation(data);
        m.put("cf", "cq", "value");
        batchWriter.addMutation(m);
      }
    }
    client.tableOperations().flush(table, null, null, true);
  }
}
