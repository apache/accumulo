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
package org.apache.accumulo.test.compaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.PluginConfig;
import org.apache.accumulo.core.client.admin.compaction.ErasureCodeConfigurer;
import org.apache.accumulo.core.client.rfile.RFile;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.ipc.RemoteException;
import org.junit.jupiter.api.Test;

public class ErasureCodeIT extends ConfigurableMacBase {
  @Override
  protected void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setNumTservers(1);
    cfg.useMiniDFS(true, 5);
  }

  List<String> getECPolicies(DistributedFileSystem dfs, ClientContext ctx, String table)
      throws Exception {
    var ample = ctx.getAmple();
    var tableId = ctx.getTableId(table);

    var policies = new ArrayList<String>();

    try (var tablets =
        ample.readTablets().forTable(tableId).fetch(TabletMetadata.ColumnType.FILES).build()) {
      for (var tabletMeta : tablets) {
        for (var file : tabletMeta.getFiles()) {
          var policy = dfs.getErasureCodingPolicy(file.getPath());
          if (policy != null) {
            policies.add(policy.getName());
          } else {
            policies.add("none");
          }
        }
      }
    }

    return policies;
  }

  private Path getTableDir(ClientContext ctx, String table) throws Exception {
    var ample = ctx.getAmple();
    var tableId = ctx.getTableId(table);

    try (var tablets =
        ample.readTablets().forTable(tableId).fetch(TabletMetadata.ColumnType.FILES).build()) {
      for (var tabletMeta : tablets) {
        for (var file : tabletMeta.getFiles()) {
          // take the tablets first file and use that to get the dir
          var path = file.getPath().getParent().getParent();
          // check the assumption of the above code
          assertTrue(path.toString().endsWith(tableId.canonical()));
          return path;

        }
      }
    }

    throw new IllegalStateException("table " + table + " has no files");
  }

  @Test
  public void test() throws Exception {
    var names = getUniqueNames(3);
    var table1 = names[0];
    var table2 = names[1];
    var table3 = names[2];
    try (AccumuloClient c = Accumulo.newClient().from(getClientProperties()).build()) {

      var policy1 = "XOR-2-1-1024k";
      var policy2 = "RS-3-2-1024k";
      var dfs = getCluster().getMiniDfs().getFileSystem();
      var configuredPolicies = dfs.getAllErasureCodingPolicies().stream()
          .map(ecpi -> ecpi.getPolicy().getName()).collect(Collectors.toSet());
      assertTrue(configuredPolicies.contains(policy1));
      assertTrue(configuredPolicies.contains(policy2));
      dfs.enableErasureCodingPolicy(policy1);
      dfs.enableErasureCodingPolicy(policy2);

      var options = Map.of(Property.TABLE_ERASURE_CODE_POLICY.getKey(), policy1,
          Property.TABLE_ENABLE_ERASURE_CODES.getKey(), "enable");
      c.tableOperations().create(table1, new NewTableConfiguration().setProperties(options));

      var options2 = Map.of(Property.TABLE_ERASURE_CODE_POLICY.getKey(), policy1,
          Property.TABLE_ENABLE_ERASURE_CODES.getKey(), "inherited");
      c.tableOperations().create(table2, new NewTableConfiguration().setProperties(options2));

      var options3 = Map.of(Property.TABLE_ENABLE_ERASURE_CODES.getKey(), "disable");
      c.tableOperations().create(table3, new NewTableConfiguration().setProperties(options3));

      SecureRandom random = new SecureRandom();

      try (var writer = c.createMultiTableBatchWriter()) {
        byte[] bytes = new byte[50_000];
        Mutation m = new Mutation("xyx");
        random.nextBytes(bytes);
        m.at().family("r").qualifier("d").put(bytes);
        writer.getBatchWriter(table1).addMutation(m);
        writer.getBatchWriter(table2).addMutation(m);
        writer.getBatchWriter(table3).addMutation(m);

        m = new Mutation("xyz");
        random.nextBytes(bytes);
        m.at().family("r").qualifier("d").put(bytes);
        writer.getBatchWriter(table1).addMutation(m);
        writer.getBatchWriter(table2).addMutation(m);
        writer.getBatchWriter(table3).addMutation(m);
      }
      c.tableOperations().flush(table1, null, null, true);
      c.tableOperations().flush(table2, null, null, true);
      c.tableOperations().flush(table3, null, null, true);

      var ctx = ((ClientContext) c);

      assertEquals(List.of(policy1), getECPolicies(dfs, ctx, table1));
      assertEquals(List.of("none"), getECPolicies(dfs, ctx, table2));
      assertEquals(List.of("none"), getECPolicies(dfs, ctx, table3));

      // This should cause the table to compact w/o erasure coding even though its configured on the
      // table
      var cconfig = new CompactionConfig()
          .setConfigurer(new PluginConfig(ErasureCodeConfigurer.class.getName(),
              Map.of(ErasureCodeConfigurer.BYPASS_ERASURE_CODES, "true")))
          .setWait(true);
      c.tableOperations().compact(table1, cconfig);
      assertEquals(List.of("none"), getECPolicies(dfs, ctx, table1));

      // table2 does not have erasure coding configured, this should cause it to compact w/ erasure
      // coding because its file should be >10K.
      cconfig = new CompactionConfig()
          .setConfigurer(new PluginConfig(ErasureCodeConfigurer.class.getName(),
              Map.of(ErasureCodeConfigurer.ERASURE_CODE_SIZE, "10K")))
          .setWait(true);
      c.tableOperations().compact(table2, cconfig);
      assertEquals(List.of(policy1), getECPolicies(dfs, ctx, table2));

      // table2 has a file around 100K in size, it should not use erasure coding because its less
      // than 1M
      cconfig = new CompactionConfig()
          .setConfigurer(new PluginConfig(ErasureCodeConfigurer.class.getName(),
              Map.of(ErasureCodeConfigurer.ERASURE_CODE_SIZE, "1M")))
          .setWait(true);
      c.tableOperations().compact(table2, cconfig);
      assertEquals(List.of("none"), getECPolicies(dfs, ctx, table1));

      // set a different policy for this compaction than what is configured on the table
      cconfig = new CompactionConfig()
          .setConfigurer(new PluginConfig(ErasureCodeConfigurer.class.getName(),
              Map.of(ErasureCodeConfigurer.ERASURE_CODE_POLICY, policy2,
                  ErasureCodeConfigurer.ERASURE_CODE_SIZE, "10K")))
          .setWait(true);
      c.tableOperations().compact(table1, cconfig);
      assertEquals(List.of(policy2), getECPolicies(dfs, ctx, table1));

      // table1 has erasure coding enabled for the table, this should override that and disable
      // erasure coding for the compaction
      cconfig = new CompactionConfig()
          .setConfigurer(new PluginConfig(ErasureCodeConfigurer.class.getName(),
              Map.of(ErasureCodeConfigurer.ERASURE_CODE_SIZE, "1M")))
          .setWait(true);
      c.tableOperations().compact(table1, cconfig);
      assertEquals(List.of("none"), getECPolicies(dfs, ctx, table1));

      // add new files to the tables
      try (var writer = c.createMultiTableBatchWriter()) {
        byte[] bytes = new byte[10_000];
        random.nextBytes(bytes);
        Mutation m = new Mutation("xyx");
        m.at().family("r2").qualifier("d").put(bytes);
        writer.getBatchWriter(table1).addMutation(m);
        writer.getBatchWriter(table2).addMutation(m);
      }
      c.tableOperations().flush(table1, null, null, true);
      c.tableOperations().flush(table2, null, null, true);

      assertEquals(List.of("none", policy1), getECPolicies(dfs, ctx, table1));
      assertEquals(List.of("none", "none"), getECPolicies(dfs, ctx, table2));

      // set the table dir erasure coding policy for all tables
      dfs.setErasureCodingPolicy(getTableDir(ctx, table1), policy2);
      dfs.setErasureCodingPolicy(getTableDir(ctx, table2), policy2);
      dfs.setErasureCodingPolicy(getTableDir(ctx, table3), policy2);
      // compact all the tables and see how setting an EC policy on the table dir influenced the
      // files created
      c.tableOperations().compact(table1, new CompactionConfig().setWait(true));
      c.tableOperations().compact(table2, new CompactionConfig().setWait(true));
      c.tableOperations().compact(table3, new CompactionConfig().setWait(true));
      // the table settings specify policy1 so that should win
      assertEquals(List.of(policy1), getECPolicies(dfs, ctx, table1));
      // the table settings specify to use the dfs dir settings so that should win
      assertEquals(List.of(policy2), getECPolicies(dfs, ctx, table2));
      // the table setting specify to use replication so that should win
      assertEquals(List.of("none"), getECPolicies(dfs, ctx, table3));

      // unset the EC policy on all table dirs
      dfs.unsetErasureCodingPolicy(getTableDir(ctx, table1));
      dfs.unsetErasureCodingPolicy(getTableDir(ctx, table2));
      dfs.unsetErasureCodingPolicy(getTableDir(ctx, table3));
      // compact all the tables and see what happens
      c.tableOperations().compact(table1, new CompactionConfig().setWait(true));
      c.tableOperations().compact(table2, new CompactionConfig().setWait(true));
      c.tableOperations().compact(table3, new CompactionConfig().setWait(true));
      // the table settings specify policy1 so that should win
      assertEquals(List.of(policy1), getECPolicies(dfs, ctx, table1));
      // the table settings specify to use the dfs dir settings so that should win and iit should
      // replicate
      assertEquals(List.of("none"), getECPolicies(dfs, ctx, table2));
      // the table setting specify to use replication and so do the directory settings
      assertEquals(List.of("none"), getECPolicies(dfs, ctx, table3));

      // test configuring an invalid policy and ensure file creation fails
      dfs.mkdirs(new Path("/tmp"));
      var badOptions = Map.of(Property.TABLE_ERASURE_CODE_POLICY.getKey(), "ycilop",
          Property.TABLE_ENABLE_ERASURE_CODES.getKey(), "enable");
      var exp = assertThrows(RemoteException.class, () -> RFile.newWriter().to("/tmp/test1.rf")
          .withFileSystem(dfs).withTableProperties(badOptions).build());
      assertTrue(exp.getMessage().contains("ycilop"));

      // Enable erasure coding but do not set a policy. The default value for the policy is empty
      // string, so this should cause a failure.
      var badOptions2 = Map.of(Property.TABLE_ENABLE_ERASURE_CODES.getKey(), "enable");
      var exp3 = assertThrows(IllegalArgumentException.class, () -> RFile.newWriter()
          .to("/tmp/test2.rf").withFileSystem(dfs).withTableProperties(badOptions2).build());
      assertTrue(exp3.getMessage().contains("empty")
          && exp3.getMessage().contains(Property.TABLE_ERASURE_CODE_POLICY.getKey()));
      // Verify assumption about default value
      assertEquals("", Property.TABLE_ERASURE_CODE_POLICY.getDefaultValue());

      // try setting invalid EC policy on table, should fail
      var exp2 = assertThrows(AccumuloException.class, () -> c.tableOperations().setProperty(table1,
          Property.TABLE_ERASURE_CODE_POLICY.getKey(), "ycilop"));
      assertTrue(exp2.getMessage().contains("ycilop"));
      assertEquals(policy1, c.tableOperations().getConfiguration(table1)
          .get(Property.TABLE_ERASURE_CODE_POLICY.getKey()));
      // should be able to set a valid policy
      c.tableOperations().setProperty(table1, Property.TABLE_ERASURE_CODE_POLICY.getKey(), policy2);
      assertEquals(policy2, c.tableOperations().getConfiguration(table1)
          .get(Property.TABLE_ERASURE_CODE_POLICY.getKey()));
    }
  }
}
