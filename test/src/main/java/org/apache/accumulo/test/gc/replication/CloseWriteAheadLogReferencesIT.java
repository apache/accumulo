/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.test.gc.replication;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.ReplicationSection;
import org.apache.accumulo.core.protobuf.ProtobufUtil;
import org.apache.accumulo.core.replication.ReplicationSchema.StatusSection;
import org.apache.accumulo.core.replication.ReplicationTable;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.gc.replication.CloseWriteAheadLogReferences;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.replication.StatusUtil;
import org.apache.accumulo.server.replication.proto.Replication.Status;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.io.Text;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Iterables;

@Ignore("Replication ITs are not stable and not currently maintained")
public class CloseWriteAheadLogReferencesIT extends ConfigurableMacBase {

  private WrappedCloseWriteAheadLogReferences refs;
  private AccumuloClient client;

  private static class WrappedCloseWriteAheadLogReferences extends CloseWriteAheadLogReferences {
    public WrappedCloseWriteAheadLogReferences(ServerContext context) {
      super(context);
    }

    @Override
    protected long updateReplicationEntries(AccumuloClient client, Set<String> closedWals) {
      return super.updateReplicationEntries(client, closedWals);
    }
  }

  @Before
  public void setupInstance() throws Exception {
    client = Accumulo.newClient().from(getClientProperties()).build();
    client.securityOperations().grantTablePermission(client.whoami(), ReplicationTable.NAME,
        TablePermission.WRITE);
    client.securityOperations().grantTablePermission(client.whoami(), MetadataTable.NAME,
        TablePermission.WRITE);
    ReplicationTable.setOnline(client);
  }

  @After
  public void teardownInstance() {
    client.close();
  }

  @Before
  public void setupEasyMockStuff() {
    SiteConfiguration siteConfig = EasyMock.createMock(SiteConfiguration.class);
    final AccumuloConfiguration systemConf = new ConfigurationCopy(new HashMap<>());

    // Just make the SiteConfiguration delegate to our AccumuloConfiguration
    // Presently, we only need get(Property) and iterator().
    EasyMock.expect(siteConfig.get(EasyMock.anyObject(Property.class))).andAnswer(() -> {
      Object[] args = EasyMock.getCurrentArguments();
      return systemConf.get((Property) args[0]);
    }).anyTimes();
    EasyMock.expect(siteConfig.getBoolean(EasyMock.anyObject(Property.class))).andAnswer(() -> {
      Object[] args = EasyMock.getCurrentArguments();
      return systemConf.getBoolean((Property) args[0]);
    }).anyTimes();

    EasyMock.expect(siteConfig.iterator()).andAnswer(() -> systemConf.iterator()).anyTimes();
    ServerContext context = createMock(ServerContext.class);
    expect(context.getProperties()).andReturn(new Properties()).anyTimes();
    expect(context.getZooKeepers()).andReturn("localhost").anyTimes();
    expect(context.getInstanceName()).andReturn("test").anyTimes();
    expect(context.getZooKeepersSessionTimeOut()).andReturn(30000).anyTimes();
    expect(context.getInstanceID()).andReturn("1111").anyTimes();
    expect(context.getZooKeeperRoot()).andReturn(Constants.ZROOT + "/1111").anyTimes();

    replay(siteConfig, context);

    refs = new WrappedCloseWriteAheadLogReferences(context);
  }

  @Test
  public void unclosedWalsLeaveStatusOpen() throws Exception {
    Set<String> wals = Collections.emptySet();
    try (BatchWriter bw = client.createBatchWriter(MetadataTable.NAME)) {
      Mutation m =
          new Mutation(ReplicationSection.getRowPrefix() + "file:/accumulo/wal/tserver+port/12345");
      m.put(ReplicationSection.COLF, new Text("1"),
          StatusUtil.fileCreatedValue(System.currentTimeMillis()));
      bw.addMutation(m);
    }

    refs.updateReplicationEntries(client, wals);

    try (Scanner s = client.createScanner(MetadataTable.NAME, Authorizations.EMPTY)) {
      s.fetchColumnFamily(ReplicationSection.COLF);
      Entry<Key,Value> entry = Iterables.getOnlyElement(s);
      Status status = Status.parseFrom(entry.getValue().get());
      assertFalse(status.getClosed());
    }
  }

  @Test
  public void closedWalsUpdateStatus() throws Exception {
    String file = "file:/accumulo/wal/tserver+port/12345";
    Set<String> wals = Collections.singleton(file);
    try (BatchWriter bw = client.createBatchWriter(MetadataTable.NAME)) {
      Mutation m = new Mutation(ReplicationSection.getRowPrefix() + file);
      m.put(ReplicationSection.COLF, new Text("1"),
          StatusUtil.fileCreatedValue(System.currentTimeMillis()));
      bw.addMutation(m);
    }

    refs.updateReplicationEntries(client, wals);

    try (Scanner s = client.createScanner(MetadataTable.NAME)) {
      s.fetchColumnFamily(ReplicationSection.COLF);
      Entry<Key,Value> entry = Iterables.getOnlyElement(s);
      Status status = Status.parseFrom(entry.getValue().get());
      assertTrue(status.getClosed());
    }
  }

  @Test
  public void partiallyReplicatedReferencedWalsAreNotClosed() throws Exception {
    String file = "file:/accumulo/wal/tserver+port/12345";
    Set<String> wals = Collections.singleton(file);
    try (BatchWriter bw = ReplicationTable.getBatchWriter(client)) {
      Mutation m = new Mutation(file);
      StatusSection.add(m, TableId.of("1"), ProtobufUtil.toValue(StatusUtil.ingestedUntil(1000)));
      bw.addMutation(m);
    }

    refs.updateReplicationEntries(client, wals);

    try (Scanner s = ReplicationTable.getScanner(client)) {
      Entry<Key,Value> entry = Iterables.getOnlyElement(s);
      Status status = Status.parseFrom(entry.getValue().get());
      assertFalse(status.getClosed());
    }
  }
}
