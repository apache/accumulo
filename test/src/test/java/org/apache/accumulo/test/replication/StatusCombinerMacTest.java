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
package org.apache.accumulo.test.replication;

import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.protobuf.ProtobufUtil;
import org.apache.accumulo.core.replication.ReplicationSchema.StatusSection;
import org.apache.accumulo.core.replication.StatusUtil;
import org.apache.accumulo.core.replication.proto.Replication.Status;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.server.replication.ReplicationTable;
import org.apache.accumulo.test.functional.SimpleMacIT;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Iterables;

/**
 * 
 */
public class StatusCombinerMacTest extends SimpleMacIT {

  @Test
  public void test() throws Exception {
    Connector conn = getConnector();
    if (conn.tableOperations().exists(ReplicationTable.NAME)) {
      conn.tableOperations().delete(ReplicationTable.NAME);
    }

    ReplicationTable.create(conn);

    BatchWriter bw = conn.createBatchWriter(ReplicationTable.NAME, new BatchWriterConfig());
    try {
      Mutation m = new Mutation("file:/accumulo/wal/HW10447.local+56808/93cdc17e-7521-44fa-87b5-37f45bcb92d3");
      StatusSection.add(m, new Text("1"), StatusUtil.newFileValue());
      bw.addMutation(m);
    } finally {
      bw.close();
    }

    Scanner s = conn.createScanner(ReplicationTable.NAME, new Authorizations());
    Entry<Key,Value> entry = Iterables.getOnlyElement(s);
    Assert.assertEquals(entry.getValue(), StatusUtil.newFileValue());

    bw = conn.createBatchWriter(ReplicationTable.NAME, new BatchWriterConfig());
    try {
      Mutation m = new Mutation("file:/accumulo/wal/HW10447.local+56808/93cdc17e-7521-44fa-87b5-37f45bcb92d3");
      StatusSection.add(m, new Text("1"), ProtobufUtil.toValue(StatusUtil.replicated(Long.MAX_VALUE)));
      bw.addMutation(m);
    } finally {
      bw.close();
    }

    s = conn.createScanner(ReplicationTable.NAME, new Authorizations());
    entry = Iterables.getOnlyElement(s);
    Status stat = Status.parseFrom(entry.getValue().get());
    Assert.assertEquals(Long.MAX_VALUE, stat.getBegin());
  }

}
