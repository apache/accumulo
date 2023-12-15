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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.constraints.Constraint;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.jupiter.api.Test;

public class WriteAfterCloseIT extends AccumuloClusterHarness {

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(Property.MANAGER_RECOVERY_DELAY, "1s");
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "10s");
    hadoopCoreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
  }

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofSeconds(300);
  }

  public static class SleepyConstraint implements Constraint {

    private static final SecureRandom rand = new SecureRandom();

    @Override
    public String getViolationDescription(short violationCode) {
      return "No such violation";
    }

    @Override
    public List<Short> check(Environment env, Mutation mutation) {

      if (mutation.getUpdates().stream().anyMatch(ColumnUpdate::isDeleted)) {
        // only want to randomly sleep for inserts, not deletes
        return null;
      }

      // the purpose of this constraint is to just randomly hold up inserts on the server side
      if (rand.nextBoolean()) {
        try {
          Thread.sleep(4000);
        } catch (InterruptedException ex) {
          throw new IllegalStateException("Interrupted during sleep", ex);
        }
      }

      return null;
    }
  }

  @Test
  public void testWriteAfterCloseMillisTime() throws Exception {
    runTest(TimeType.MILLIS, false, 0, false);
  }

  @Test
  public void testWriteAfterCloseLogicalTime() throws Exception {
    runTest(TimeType.LOGICAL, false, 0, false);
  }

  @Test
  public void testWriteAfterCloseKillTservers() throws Exception {
    runTest(TimeType.MILLIS, true, 0, false);
  }

  @Test
  public void testWriteAfterCloseTimeout() throws Exception {
    // ensure that trying to close seesions does not interfere with timeout
    runTest(TimeType.MILLIS, false, 2000, true);
  }

  private void runTest(TimeType timeType, boolean killTservers, long timeout, boolean expectErrors)
      throws Exception {
    // re #3721 test that tries to cause a write event to happen after a batch writer is closed
    String table = getUniqueNames(1)[0];
    var props = new Properties();
    props.putAll(getClientProps());
    props.setProperty(Property.GENERAL_RPC_TIMEOUT.getKey(), "1s");

    NewTableConfiguration ntc = new NewTableConfiguration().setTimeType(timeType);
    ntc.setProperties(
        Map.of(Property.TABLE_CONSTRAINT_PREFIX.getKey() + "1", SleepyConstraint.class.getName()));

    // The short rpc timeout and the random sleep in the constraint can cause some of the writes
    // done by a batch writer to timeout. The batch writer will internally retry the write, but the
    // timed out write could still go through at a later time.

    var executor = Executors.newCachedThreadPool();

    try (AccumuloClient c = Accumulo.newClient().from(props).build()) {
      c.tableOperations().create(table, ntc);

      List<Future<?>> futures = new ArrayList<>();

      for (int i = 0; i < 100; i++) {
        futures.add(executor.submit(createWriteTask(i * 1000, c, table, timeout)));
      }

      if (killTservers) {
        Thread.sleep(250);
        getCluster().getClusterControl().stopAllServers(ServerType.TABLET_SERVER);
        // sleep longer than ZK timeout to let ephemeral lock nodes expire in ZK
        Thread.sleep(11000);
        getCluster().getClusterControl().startAllServers(ServerType.TABLET_SERVER);
      }

      int errorCount = 0;

      // wait for all futures to complete
      for (var future : futures) {
        try {
          future.get();
        } catch (ExecutionException e) {
          errorCount++;
        }
      }

      if (expectErrors) {
        assertTrue(errorCount > 0);
      } else {
        assertEquals(0, errorCount);

        try (Scanner scanner = c.createScanner(table)) {
          // every insertion was deleted so table should be empty unless there were out of order
          // writes
          assertEquals(0, scanner.stream().count());
        }
      }
    } finally {
      executor.shutdownNow();
    }
  }

  private static Callable<Void> createWriteTask(int row, AccumuloClient c, String table,
      long timeout) {
    return () -> {

      BatchWriterConfig bwc = new BatchWriterConfig().setTimeout(timeout, TimeUnit.MILLISECONDS);

      try (BatchWriter writer = c.createBatchWriter(table, bwc)) {
        Mutation m = new Mutation("r" + row);
        m.put("f1", "q1", new Value("v1"));
        writer.addMutation(m);
      }

      // Relying on the internal retries of the batch writer, trying to create a situation where
      // some of the writes from above actually happen after the delete below which would negate the
      // delete.

      try (BatchWriter writer = c.createBatchWriter(table)) {
        Mutation m = new Mutation("r" + row);
        m.putDelete("f1", "q1");
        writer.addMutation(m);
      }
      return null;
    };
  }
}
