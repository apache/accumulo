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
package org.apache.accumulo.master.replication;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.ReplicationSection;
import org.apache.accumulo.core.replication.ReplicationSchema.StatusSection;
import org.apache.accumulo.core.replication.StatusUtil;
import org.apache.accumulo.core.replication.proto.Replication.Status;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.server.replication.ReplicationTable;
import org.apache.accumulo.server.util.ReplicationTableUtil;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import com.google.common.collect.Sets;

/**
 * 
 */
public class StatusMakerTest {

  @Rule
  public TestName test = new TestName();

  @Test
  public void statusRecordsCreated() throws Exception {
    MockInstance inst = new MockInstance(test.getMethodName());
    Credentials creds = new Credentials("root", new PasswordToken(""));
    Connector conn = inst.getConnector(creds.getPrincipal(), creds.getToken());

    String sourceTable = "source";
    conn.tableOperations().create(sourceTable);
    ReplicationTableUtil.configureMetadataTable(conn, sourceTable);

    BatchWriter bw = conn.createBatchWriter(sourceTable, new BatchWriterConfig());
    String walPrefix = "hdfs://localhost:8020/accumulo/wals/tserver+port/";
    Set<String> files = Sets.newHashSet(walPrefix + UUID.randomUUID(), walPrefix + UUID.randomUUID(), walPrefix + UUID.randomUUID(),
        walPrefix + UUID.randomUUID());
    Map<String,Integer> fileToTableId = new HashMap<>();

    int index = 1;
    for (String file : files) {
      Mutation m = new Mutation(ReplicationSection.getRowPrefix() + file);
      m.put(ReplicationSection.COLF, new Text(Integer.toString(index)), StatusUtil.newFileValue());
      bw.addMutation(m);
      fileToTableId.put(file, index);
      index++;
    }

    bw.close();

    StatusMaker statusMaker = new StatusMaker(conn);
    statusMaker.setSourceTableName(sourceTable);

    statusMaker.run();

    Scanner s = ReplicationTable.getScanner(conn);
    StatusSection.limit(s);
    Text file = new Text(), tableId = new Text();
    for (Entry<Key,Value> entry : s) {
      StatusSection.getFile(entry.getKey(), file);
      StatusSection.getTableId(entry.getKey(), tableId);

      Assert.assertTrue("Found unexpected file: " + file, files.contains(file.toString()));
      Assert.assertEquals(fileToTableId.get(file.toString()), new Integer(tableId.toString()));
      Assert.assertEquals(StatusUtil.newFile(), Status.parseFrom(entry.getValue().get()));
    }
  }

}
