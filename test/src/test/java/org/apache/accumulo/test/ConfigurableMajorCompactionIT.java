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
package org.apache.accumulo.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.fate.util.UtilWaitThread;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ConfigurableMacIT;
import org.apache.accumulo.test.functional.FunctionalTestUtils;
import org.apache.accumulo.tserver.compaction.CompactionPlan;
import org.apache.accumulo.tserver.compaction.CompactionStrategy;
import org.apache.accumulo.tserver.compaction.MajorCompactionRequest;
import org.apache.accumulo.tserver.compaction.WriteParameters;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class ConfigurableMajorCompactionIT extends ConfigurableMacIT {

  @Override
  public int defaultTimeoutSeconds() {
    return 30;
  }

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    Map<String,String> siteConfig = new HashMap<String,String>();
    siteConfig.put(Property.TSERV_MAJC_DELAY.getKey(), "1s");
    cfg.setSiteConfig(siteConfig);
  }

  public static class TestCompactionStrategy extends CompactionStrategy {

    @Override
    public boolean shouldCompact(MajorCompactionRequest request) throws IOException {
      return request.getFiles().size() == 5;
    }

    @Override
    public CompactionPlan getCompactionPlan(MajorCompactionRequest request) throws IOException {
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
    Connector conn = getConnector();
    String tableName = getUniqueNames(1)[0];
    conn.tableOperations().create(tableName);
    conn.tableOperations().setProperty(tableName, Property.TABLE_COMPACTION_STRATEGY.getKey(), TestCompactionStrategy.class.getName());
    writeFile(conn, tableName);
    writeFile(conn, tableName);
    writeFile(conn, tableName);
    writeFile(conn, tableName);
    UtilWaitThread.sleep(2 * 1000);
    assertEquals(4, countFiles(conn));
    writeFile(conn, tableName);
    int count = countFiles(conn);
    assertTrue(count == 1 || count == 5);
    while (count != 1) {
      UtilWaitThread.sleep(250);
      count = countFiles(conn);
    }
  }

  private int countFiles(Connector conn) throws Exception {
    Scanner s = conn.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    s.setRange(MetadataSchema.TabletsSection.getRange());
    s.fetchColumnFamily(MetadataSchema.TabletsSection.DataFileColumnFamily.NAME);
    return FunctionalTestUtils.count(s);
  }

  private void writeFile(Connector conn, String tableName) throws Exception {
    BatchWriter bw = conn.createBatchWriter(tableName, new BatchWriterConfig());
    Mutation m = new Mutation("row");
    m.put("cf", "cq", "value");
    bw.addMutation(m);
    bw.close();
    conn.tableOperations().flush(tableName, null, null, true);
  }

}
