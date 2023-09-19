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
package org.apache.accumulo.test;

import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily.DIRECTORY_QUAL;
import static org.apache.accumulo.harness.AccumuloITBase.MINI_CLUSTER_ONLY;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.admin.CloneConfiguration;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.gc.Reference;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.gc.GCRun;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(MINI_CLUSTER_ONLY)
public class GCRunIT extends SharedMiniClusterBase {

  public static final Logger log = LoggerFactory.getLogger(GCRunIT.class);

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofSeconds(30);
  }

  @BeforeAll
  public static void setup() throws Exception {
    SharedMiniClusterBase.startMiniCluster();
  }

  @AfterAll
  public static void teardown() {
    SharedMiniClusterBase.stopMiniCluster();
  }

  /**
   * Simple run through GCRun candidate to validate that other tests change behaviour when error
   * injected.
   */
  @Test
  public void goPath() throws Exception {
    final String[] names = getUniqueNames(2);
    final String table1 = names[0];
    final String clone1 = names[1];

    fillMetadataEntries(table1, clone1);

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {

      var context = getCluster().getServerContext();

      scanReferences(new GCRun(Ample.DataLevel.ROOT, context));
      scanReferences(new GCRun(Ample.DataLevel.METADATA, context));
      scanReferences(new GCRun(Ample.DataLevel.USER, context));

      client.tableOperations().delete(clone1);
    }
  }

  /**
   * Force deletion of dir entry from the metadata table - expect GCRun to throw exception scanning
   * candidate
   */
  @Test
  public void forceMissingDirTest() throws Exception {

    final String[] names = getUniqueNames(2);
    final String table1 = names[0];
    final String clone1 = names[1];

    fillMetadataEntries(table1, clone1);
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {

      client.securityOperations().grantTablePermission(getAdminPrincipal(), MetadataTable.NAME,
          TablePermission.WRITE);

      String cloneId = client.tableOperations().tableIdMap().get(clone1);

      // pick last row that should always exist.
      final Mutation m = new Mutation(new Text(cloneId + "<"));
      final Text colf = new Text(MetadataSchema.TabletsSection.ServerColumnFamily.NAME);
      final Text colq = new Text(DIRECTORY_QUAL);
      m.putDelete(colf, colq, new ColumnVisibility());

      try (BatchWriter bw = client.createBatchWriter(MetadataTable.NAME,
          new BatchWriterConfig().setMaxMemory(Math.max(m.estimatedMemoryUsed(), 1024))
              .setMaxWriteThreads(1).setTimeout(5_000, TimeUnit.MILLISECONDS))) {
        log.info("forcing delete of srv:dir with mutation {}", m.prettyPrint());
        bw.addMutation(m);
      }

      var context = getCluster().getServerContext();

      scanReferences(new GCRun(Ample.DataLevel.ROOT, context));
      scanReferences(new GCRun(Ample.DataLevel.METADATA, context));
      // "missing srv:dir prevents full reference scan for user table
      assertThrows(IllegalStateException.class,
          () -> scanReferences(new GCRun(Ample.DataLevel.USER, context)));

      client.tableOperations().delete(clone1);
    }
  }

  /**
   * This is a placeholder - deleting the prev row causes the scan to fail before validation check.
   * Either find a way to simulate / force or delete this test. To test, Ample or the methods in
   * GCRun need to support injecting synthetic row data, or another solution is required.
   */
  @Test
  @Disabled("deleting prev row causes scan to fail before row read validation")
  public void forceMissingPrevRowTest() {}

  private void scanReferences(GCRun userGC) {
    final AtomicInteger counter = new AtomicInteger(0);
    // loop through the user table references - the row deleted above should violate dir present.
    var userTableIter = userGC.getReferences().iterator();
    while (userTableIter.hasNext()) {
      Reference ref = userTableIter.next();
      counter.incrementAndGet();
      log.trace("user ref: {}", ref);
    }

    assertTrue(counter.get() > 0);
  }

  private void fillMetadataEntries(final String table1, final String clone1) throws Exception {
    getCluster().getClusterControl().stop(ServerType.GARBAGE_COLLECTOR);

    TreeSet<Text> splits = new TreeSet<>(List.of(new Text("3"), new Text("5"), new Text("7")));
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {

      NewTableConfiguration ntc = new NewTableConfiguration().withSplits(splits);
      ntc.withSplits(splits);
      client.tableOperations().create(table1, ntc);
      client.tableOperations().compact(table1, new CompactionConfig().setWait(true));

      BatchWriterConfig config = new BatchWriterConfig();
      config.setMaxMemory(0);
      try (BatchWriter writer = client.createBatchWriter(table1, config)) {
        for (int i = 0; i < 10; i++) {
          Mutation m = new Mutation(i + "_row");
          m.put("cf", "cq", new Value("value " + i));
          writer.addMutation(m);
        }
      }
      client.tableOperations().flush(table1, null, null, true);
      client.tableOperations().clone(table1, clone1, CloneConfiguration.empty());

      client.tableOperations().compact(table1, new CompactionConfig().setWait(true));
      client.tableOperations().delete(table1);

      client.tableOperations().flush(MetadataTable.NAME, null, null, true);
      client.tableOperations().flush(RootTable.NAME, null, null, true);

    }
  }
}
