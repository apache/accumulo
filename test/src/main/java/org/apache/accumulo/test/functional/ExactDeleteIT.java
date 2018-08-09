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

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.client.impl.OfflineScanner;
import org.apache.accumulo.core.client.impl.Table.ID;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.constraints.NoTimestampDeleteConstraint;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;

public class ExactDeleteIT extends AccumuloClusterHarness {

  void verifyVals(Scanner scanner, String... expectedArray) {
    Set<String> actual = StreamSupport.stream(scanner.spliterator(), false)
        .map(e -> e.getValue().toString()).collect(Collectors.toSet());
    Set<String> expected = ImmutableSet.copyOf(expectedArray);

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testScanAndCompact() throws Exception {
    Connector c = getConnector();
    String tableName = getUniqueNames(1)[0];
    NewTableConfiguration ntc = new NewTableConfiguration();
    ntc.setExactDeleteEnabled(true);
    ntc.withoutDefaultIterators();
    c.tableOperations().create(tableName, ntc);

    Assert.assertTrue(c.tableOperations().isExactDeleteEnabled(tableName));

    writeInitialData(c, tableName);

    Scanner scanner = c.createScanner(tableName, Authorizations.EMPTY);

    verifyVals(scanner, "v0", "v1", "v2", "v3", "v10", "v11", "v12", "v13");

    try (BatchWriter writer = c.createBatchWriter(tableName)) {
      Mutation m = new Mutation("1234");
      m.putDelete("f1", "q1", 2L);
      writer.addMutation(m);

      m = new Mutation("1235");
      m.putDelete("f1", "q1", 1L);
      writer.addMutation(m);

      m = new Mutation("1235");
      m.putDelete("f1", "q1", 3L);
      writer.addMutation(m);
    }

    verifyVals(scanner, "v0", "v1", "v3", "v10", "v12");

    c.tableOperations().flush(tableName, null, null, true);

    verifyVals(scanner, "v0", "v1", "v3", "v10", "v12");

    try (BatchWriter writer = c.createBatchWriter(tableName)) {
      Mutation m = new Mutation("1234");
      m.putDelete("f1", "q1", 3L);
      writer.addMutation(m);
    }

    // should have a delete in memory for data in files
    verifyVals(scanner, "v0", "v1", "v10", "v12");

    c.tableOperations().offline(tableName, true);
    // Should be two files at this point, with a delete in one file for an entry in another file.
    // Having these two files is important for testing offline scan and compaction. Do not want a
    // single file where deletes were processed.
    ClientContext ctx = new ClientContext(c.info());
    ID tid = Tables.getTableId(ctx, tableName);
    OfflineScanner offlineScanner = new OfflineScanner(new ClientContext(c.info()), tid,
        Authorizations.EMPTY);
    verifyVals(offlineScanner, "v0", "v1", "v10", "v12");
    c.tableOperations().online(tableName, true);

    c.tableOperations().compact(tableName, new CompactionConfig().setFlush(true).setWait(true));

    verifyVals(scanner, "v0", "v1", "v10", "v12");
  }

  private void writeInitialData(Connector c, String tableName)
      throws MutationsRejectedException, TableNotFoundException {
    try (BatchWriter writer = c.createBatchWriter(tableName)) {
      for (long l = 0; l < 4; l++) {
        Mutation m = new Mutation("1234");
        m.put("f1", "q1", l, "v" + l);
        writer.addMutation(m);

        m = new Mutation("1235");
        m.put("f1", "q1", l, "v" + (10 + l));
        writer.addMutation(m);
      }
    }
  }

  @Test
  public void testClone() throws Exception {
    Connector c = getConnector();
    String[] tableNames = getUniqueNames(4);
    String tableName = tableNames[0];
    String normalTable = tableNames[1];
    NewTableConfiguration ntc = new NewTableConfiguration();
    ntc.setExactDeleteEnabled(true);
    ntc.withoutDefaultIterators();
    c.tableOperations().create(tableName, ntc);

    ntc = new NewTableConfiguration();
    ntc.withoutDefaultIterators();
    c.tableOperations().create(normalTable, ntc);

    Assert.assertTrue(c.tableOperations().isExactDeleteEnabled(tableName));
    Assert.assertFalse(c.tableOperations().isExactDeleteEnabled(normalTable));

    writeInitialData(c, tableName);
    writeInitialData(c, normalTable);

    String cloneExactDel = tableNames[2];
    String cloneNormal = tableNames[3];
    c.tableOperations().clone(tableName, cloneExactDel, true, Collections.emptyMap(),
        Collections.emptySet());

    c.tableOperations().clone(normalTable, cloneNormal, true, Collections.emptyMap(),
        Collections.emptySet());

    Assert.assertTrue(c.tableOperations().isExactDeleteEnabled(cloneExactDel));
    Assert.assertFalse(c.tableOperations().isExactDeleteEnabled(cloneNormal));

    Scanner scanner1 = c.createScanner(cloneExactDel, Authorizations.EMPTY);
    Scanner scanner2 = c.createScanner(cloneNormal, Authorizations.EMPTY);

    verifyVals(scanner1, "v0", "v1", "v2", "v3", "v10", "v11", "v12", "v13");
    verifyVals(scanner2, "v0", "v1", "v2", "v3", "v10", "v11", "v12", "v13");

    for (String table : new String[] {cloneExactDel, cloneNormal}) {
      try (BatchWriter writer = c.createBatchWriter(table)) {
        Mutation m = new Mutation("1234");
        m.putDelete("f1", "q1", 2L);
        writer.addMutation(m);

        m = new Mutation("1235");
        m.putDelete("f1", "q1", 1L);
        writer.addMutation(m);

        m = new Mutation("1235");
        m.putDelete("f1", "q1", 3L);
        writer.addMutation(m);
      }
    }

    verifyVals(scanner1, "v0", "v1", "v3", "v10", "v12");
    verifyVals(scanner2, "v3");
  }

  @Test
  public void testConstraint() throws Exception {
    Connector c = getConnector();
    String tableName = getUniqueNames(1)[0];
    NewTableConfiguration ntc = new NewTableConfiguration();
    ntc.setProperties(Collections.singletonMap("table.constraint.1",
        NoTimestampDeleteConstraint.class.getName()));
    ntc.setExactDeleteEnabled(true);
    ntc.withoutDefaultIterators();
    c.tableOperations().create(tableName, ntc);

    // Following write should not be affected by constraint
    writeInitialData(c, tableName);

    Scanner scanner = c.createScanner(tableName, Authorizations.EMPTY);

    verifyVals(scanner, "v0", "v1", "v2", "v3", "v10", "v11", "v12", "v13");

    // Following deletes should not be affected by constraint
    try (BatchWriter writer = c.createBatchWriter(tableName)) {
      Mutation m = new Mutation("1234");
      m.putDelete("f1", "q1", 2L);
      writer.addMutation(m);

      m = new Mutation("1235");
      m.putDelete("f1", "q1", 1L);
      writer.addMutation(m);
    }

    verifyVals(scanner, "v0", "v1", "v3", "v10", "v12", "v13");

    BatchWriter writer = c.createBatchWriter(tableName);
    try {
      Mutation m = new Mutation("1234");
      m.putDelete("f1", "q1");
      writer.addMutation(m);
      writer.close();
      Assert.fail("Expected write to fail because of constraint");
    } catch (MutationsRejectedException mre) {
      Assert.assertEquals(1, mre.getConstraintViolationSummaries().size());
      String desc = mre.getConstraintViolationSummaries().get(0).getViolationDescription();
      Assert.assertEquals("Delete did not specify a timestamp", desc);
    }

    verifyVals(scanner, "v0", "v1", "v3", "v10", "v12", "v13");
  }
}
