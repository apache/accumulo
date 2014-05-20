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
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.ReplicationSection;
import org.apache.accumulo.core.protobuf.ProtobufUtil;
import org.apache.accumulo.core.replication.StatusUtil;
import org.apache.accumulo.core.replication.proto.Replication.Status;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.util.ReplicationTableUtil;
import org.apache.accumulo.test.functional.ConfigurableMacIT;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Iterables;

/**
 * 
 */
public class ReplicationTablesMacTest extends ConfigurableMacIT {

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setNumTservers(1);
  }

  @Test
  public void combinerWorksOnMetadata() throws Exception {
    Connector conn = getConnector();

    conn.securityOperations().grantTablePermission("root", MetadataTable.NAME, TablePermission.WRITE);

    ReplicationTableUtil.configureMetadataTable(conn, MetadataTable.NAME);

    Status stat1 = StatusUtil.fileCreated(100);
    Status stat2 = StatusUtil.fileClosed();

    BatchWriter bw = conn.createBatchWriter(MetadataTable.NAME, new BatchWriterConfig());
    Mutation m = new Mutation(ReplicationSection.getRowPrefix() + "file:/accumulo/wals/tserver+port/uuid");
    m.put(ReplicationSection.COLF, new Text("1"), ProtobufUtil.toValue(stat1));
    bw.addMutation(m);
    bw.close();

    Scanner s = conn.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    s.setRange(ReplicationSection.getRange());
    System.out.println("Printing metadata table");

    Status actual = Status.parseFrom(Iterables.getOnlyElement(s).getValue().get());
    Assert.assertEquals(stat1, actual);

    bw = conn.createBatchWriter(MetadataTable.NAME, new BatchWriterConfig());
    m = new Mutation(ReplicationSection.getRowPrefix() + "file:/accumulo/wals/tserver+port/uuid");
    m.put(ReplicationSection.COLF, new Text("1"), ProtobufUtil.toValue(stat2));
    bw.addMutation(m);
    bw.close();

    s = conn.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    s.setRange(ReplicationSection.getRange());

    actual = Status.parseFrom(Iterables.getOnlyElement(s).getValue().get());
    Status expected = Status.newBuilder().setBegin(0).setEnd(0).setClosed(true).setInfiniteEnd(true).setCreatedTime(100).build();

    Assert.assertEquals(expected, actual);
  }

}
