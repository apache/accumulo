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
package org.apache.accumulo.test.functional;

import static org.apache.accumulo.test.functional.ReadWriteIT.ROWS;
import static org.apache.accumulo.test.functional.ReadWriteIT.ingest;
import static org.apache.accumulo.test.functional.ReadWriteIT.m;
import static org.apache.accumulo.test.functional.ReadWriteIT.t;
import static org.apache.accumulo.test.functional.ReadWriteIT.verify;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.accumulo.cluster.standalone.StandaloneAccumuloCluster;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.rfile.PrintInfo;
import org.apache.accumulo.core.metadata.AccumuloTable;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalityGroupIT extends AccumuloClusterHarness {

  private static final Logger log = LoggerFactory.getLogger(LocalityGroupIT.class);

  @Test
  public void localityGroupPerf() throws Exception {
    // verify that locality groups can make look-ups faster
    try (AccumuloClient accumuloClient = Accumulo.newClient().from(getClientProps()).build()) {
      final String tableName = getUniqueNames(1)[0];
      accumuloClient.tableOperations().create(tableName);
      accumuloClient.tableOperations().setProperty(tableName, "table.group.g1", "colf");
      accumuloClient.tableOperations().setProperty(tableName, "table.groups.enabled", "g1");
      ingest(accumuloClient, 2000, 1, 50, 0, tableName);
      accumuloClient.tableOperations().compact(tableName, null, null, true, true);
      try (BatchWriter bw = accumuloClient.createBatchWriter(tableName)) {
        bw.addMutation(m("zzzzzzzzzzz", "colf2", "cq", "value"));
      }
      long now = System.currentTimeMillis();
      try (Scanner scanner = accumuloClient.createScanner(tableName, Authorizations.EMPTY)) {
        scanner.fetchColumnFamily(new Text("colf"));
        scanner.forEach((k, v) -> {});
      }
      long diff = System.currentTimeMillis() - now;
      now = System.currentTimeMillis();

      try (Scanner scanner = accumuloClient.createScanner(tableName, Authorizations.EMPTY)) {
        scanner.fetchColumnFamily(new Text("colf2"));
        scanner.forEach((k, v) -> {});
      }
      long diff2 = System.currentTimeMillis() - now;
      assertTrue(diff2 < diff);
    }
  }

  /**
   * create a locality group, write to it and ensure it exists in the RFiles that result
   */
  @Test
  public void sunnyLG() throws Exception {
    try (AccumuloClient accumuloClient = Accumulo.newClient().from(getClientProps()).build()) {
      final String tableName = getUniqueNames(1)[0];
      accumuloClient.tableOperations().create(tableName);
      Map<String,Set<Text>> groups = new TreeMap<>();
      groups.put("g1", Collections.singleton(t("colf")));
      accumuloClient.tableOperations().setLocalityGroups(tableName, groups);
      verifyLocalityGroupsInRFile(accumuloClient, tableName);
    }
  }

  /**
   * Pretty much identical to sunnyLG, but verifies locality groups are created when configured in
   * NewTableConfiguration prior to table creation.
   */
  @Test
  public void sunnyLGUsingNewTableConfiguration() throws Exception {
    // create a locality group, write to it and ensure it exists in the RFiles that result
    try (AccumuloClient accumuloClient = Accumulo.newClient().from(getClientProps()).build()) {
      final String tableName = getUniqueNames(1)[0];
      NewTableConfiguration ntc = new NewTableConfiguration();
      Map<String,Set<Text>> groups = new HashMap<>();
      groups.put("g1", Collections.singleton(t("colf")));
      ntc.setLocalityGroups(groups);
      accumuloClient.tableOperations().create(tableName, ntc);
      verifyLocalityGroupsInRFile(accumuloClient, tableName);
    }
  }

  private void verifyLocalityGroupsInRFile(final AccumuloClient accumuloClient,
      final String tableName) throws Exception {
    ingest(accumuloClient, 2000, 1, 50, 0, tableName);
    verify(accumuloClient, 2000, 1, 50, 0, tableName);
    accumuloClient.tableOperations().flush(tableName, null, null, true);
    try (BatchScanner bscanner = accumuloClient
        .createBatchScanner(AccumuloTable.METADATA.tableName(), Authorizations.EMPTY, 1)) {
      String tableId = accumuloClient.tableOperations().tableIdMap().get(tableName);
      bscanner.setRanges(
          Collections.singletonList(new Range(new Text(tableId + ";"), new Text(tableId + "<"))));
      bscanner.fetchColumnFamily(MetadataSchema.TabletsSection.DataFileColumnFamily.NAME);
      boolean foundFile = false;
      for (Map.Entry<Key,Value> entry : bscanner) {
        foundFile = true;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream oldOut = System.out;
        try (PrintStream newOut = new PrintStream(baos)) {
          System.setOut(newOut);
          List<String> args = new ArrayList<>();
          args.add(StoredTabletFile.of(entry.getKey().getColumnQualifier()).getMetadataPath());
          args.add("--props");
          args.add(getCluster().getAccumuloPropertiesPath());
          if (getClusterType() == ClusterType.STANDALONE && saslEnabled()) {
            args.add("--config");
            StandaloneAccumuloCluster sac = (StandaloneAccumuloCluster) cluster;
            String hadoopConfDir = sac.getHadoopConfDir();
            args.add(new Path(hadoopConfDir, "core-site.xml").toString());
            args.add(new Path(hadoopConfDir, "hdfs-site.xml").toString());
          }
          log.info("Invoking PrintInfo with {}", args);
          PrintInfo.main(args.toArray(new String[args.size()]));
          newOut.flush();
          String stdout = baos.toString();
          assertTrue(stdout.contains("Locality group           : g1"));
          assertTrue(stdout.contains("families        : [colf]"));
        } finally {
          System.setOut(oldOut);
        }
      }
      assertTrue(foundFile);
    }
  }

  @Test
  public void localityGroupChange() throws Exception {
    // Make changes to locality groups and ensure nothing is lost
    try (AccumuloClient accumuloClient = Accumulo.newClient().from(getClientProps()).build()) {
      String table = getUniqueNames(1)[0];
      TableOperations to = accumuloClient.tableOperations();
      to.create(table);
      String[] config = {"lg1:colf", null, "lg1:colf,xyz", "lg1:colf,xyz;lg2:c1,c2"};
      int i = 0;
      for (String cfg : config) {
        to.setLocalityGroups(table, getGroups(cfg));
        ingest(accumuloClient, ROWS * (i + 1), 1, 50, ROWS * i, table);
        to.flush(table, null, null, true);
        verify(accumuloClient, 0, 1, 50, ROWS * (i + 1), table);
        i++;
      }
      to.delete(table);
      to.create(table);
      config = new String[] {"lg1:colf", null, "lg1:colf,xyz", "lg1:colf;lg2:colf",};
      i = 1;
      for (String cfg : config) {
        ingest(accumuloClient, ROWS * i, 1, 50, 0, table);
        ingest(accumuloClient, ROWS * i, 1, 50, 0, "xyz", table);
        to.setLocalityGroups(table, getGroups(cfg));
        to.flush(table, null, null, true);
        verify(accumuloClient, ROWS * i, 1, 50, 0, table);
        verify(accumuloClient, ROWS * i, 1, 50, 0, "xyz", table);
        i++;
      }
    }
  }

  private Map<String,Set<Text>> getGroups(String cfg) {
    Map<String,Set<Text>> groups = new TreeMap<>();
    if (cfg != null) {
      for (String group : cfg.split(";")) {
        String[] parts = group.split(":");
        Set<Text> cols = new HashSet<>();
        for (String col : parts[1].split(",")) {
          cols.add(t(col));
        }
        groups.put(parts[1], cols);
      }
    }
    return groups;
  }
}
