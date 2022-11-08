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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.accumulo.tserver.compaction.CompactionPlan;
import org.apache.accumulo.tserver.compaction.CompactionStrategy;
import org.apache.accumulo.tserver.compaction.MajorCompactionRequest;
import org.apache.accumulo.tserver.compaction.WriteParameters;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Iterators;

@SuppressWarnings("removal")
public class ConfigurableMajorCompactionIT extends ConfigurableMacBase {

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofSeconds(30);
  }

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    Map<String,String> siteConfig = new HashMap<>();
    siteConfig.put(Property.TSERV_MAJC_DELAY.getKey(), "1s");
    cfg.setSiteConfig(siteConfig);
  }

  public static class TestCompactionStrategy extends CompactionStrategy {

    @Override
    public boolean shouldCompact(MajorCompactionRequest request) {
      return request.getFiles().size() == 5;
    }

    @Override
    public CompactionPlan getCompactionPlan(MajorCompactionRequest request) {
      CompactionPlan plan = new CompactionPlan();
      plan.inputFiles.addAll(request.getFiles().keySet());
      plan.writeParameters = new WriteParameters();
      plan.writeParameters.setBlockSize(1024 * 1024);
      plan.writeParameters.setCompressType("none");
      plan.writeParameters.setHdfsBlockSize(1024 * 1024);
      plan.writeParameters.setIndexBlockSize(10);
      plan.writeParameters.setReplication(7);
      return plan;
    }
  }

  @Test
  public void test() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      String tableName = getUniqueNames(1)[0];
      client.tableOperations().create(tableName);
      client.tableOperations().setProperty(tableName, Property.TABLE_COMPACTION_STRATEGY.getKey(),
          TestCompactionStrategy.class.getName());
      writeFile(client, tableName);
      writeFile(client, tableName);
      writeFile(client, tableName);
      writeFile(client, tableName);
      UtilWaitThread.sleep(2_000);
      assertEquals(4, countFiles(client));
      writeFile(client, tableName);
      int count = countFiles(client);
      assertTrue(count == 1 || count == 5);
      while (count != 1) {
        UtilWaitThread.sleep(250);
        count = countFiles(client);
      }
    }
  }

  private int countFiles(AccumuloClient client) throws Exception {
    try (Scanner s = client.createScanner(MetadataTable.NAME, Authorizations.EMPTY)) {
      s.setRange(TabletsSection.getRange());
      s.fetchColumnFamily(DataFileColumnFamily.NAME);
      return Iterators.size(s.iterator());
    }
  }

  private void writeFile(AccumuloClient client, String tableName) throws Exception {
    try (BatchWriter bw = client.createBatchWriter(tableName)) {
      Mutation m = new Mutation("row");
      m.put("cf", "cq", "value");
      bw.addMutation(m);
    }
    client.tableOperations().flush(tableName, null, null, true);
  }

}
