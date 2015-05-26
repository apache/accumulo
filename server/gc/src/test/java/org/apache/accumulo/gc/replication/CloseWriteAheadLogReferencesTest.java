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
package org.apache.accumulo.gc.replication;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.ReplicationSection;
import org.apache.accumulo.core.protobuf.ProtobufUtil;
import org.apache.accumulo.core.replication.ReplicationSchema.StatusSection;
import org.apache.accumulo.core.replication.ReplicationTable;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.trace.thrift.TInfo;
import org.apache.accumulo.server.AccumuloServerContext;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.accumulo.server.replication.StatusUtil;
import org.apache.accumulo.server.replication.proto.Replication.Status;
import org.apache.hadoop.io.Text;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import com.google.common.collect.Iterables;
import com.google.common.net.HostAndPort;

public class CloseWriteAheadLogReferencesTest {

  private CloseWriteAheadLogReferences refs;
  private Instance inst;

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setup() {
    inst = createMock(Instance.class);
    SiteConfiguration siteConfig = EasyMock.createMock(SiteConfiguration.class);
    expect(inst.getInstanceID()).andReturn(testName.getMethodName()).anyTimes();
    expect(inst.getZooKeepers()).andReturn("localhost").anyTimes();
    expect(inst.getZooKeepersSessionTimeOut()).andReturn(30000).anyTimes();
    final AccumuloConfiguration systemConf = new ConfigurationCopy(new HashMap<String,String>());
    ServerConfigurationFactory factory = createMock(ServerConfigurationFactory.class);
    expect(factory.getConfiguration()).andReturn(systemConf).anyTimes();
    expect(factory.getInstance()).andReturn(inst).anyTimes();
    expect(factory.getSiteConfiguration()).andReturn(siteConfig).anyTimes();

    // Just make the SiteConfiguration delegate to our AccumuloConfiguration
    // Presently, we only need get(Property) and iterator().
    EasyMock.expect(siteConfig.get(EasyMock.anyObject(Property.class))).andAnswer(new IAnswer<String>() {
      @Override
      public String answer() {
        Object[] args = EasyMock.getCurrentArguments();
        return systemConf.get((Property) args[0]);
      }
    }).anyTimes();
    EasyMock.expect(siteConfig.getBoolean(EasyMock.anyObject(Property.class))).andAnswer(new IAnswer<Boolean>() {
      @Override
      public Boolean answer() {
        Object[] args = EasyMock.getCurrentArguments();
        return systemConf.getBoolean((Property) args[0]);
      }
    }).anyTimes();

    EasyMock.expect(siteConfig.iterator()).andAnswer(new IAnswer<Iterator<Entry<String,String>>>() {
      @Override
      public Iterator<Entry<String,String>> answer() {
        return systemConf.iterator();
      }
    }).anyTimes();

    replay(inst, factory, siteConfig);
    refs = new CloseWriteAheadLogReferences(new AccumuloServerContext(factory));
  }

  @Test
  public void unusedWalsAreClosed() throws Exception {
    Set<String> wals = Collections.emptySet();
    Instance inst = new MockInstance(testName.getMethodName());
    Connector conn = inst.getConnector("root", new PasswordToken(""));

    BatchWriter bw = conn.createBatchWriter(MetadataTable.NAME, new BatchWriterConfig());
    Mutation m = new Mutation(ReplicationSection.getRowPrefix() + "file:/accumulo/wal/tserver+port/12345");
    m.put(ReplicationSection.COLF, new Text("1"), StatusUtil.fileCreatedValue(System.currentTimeMillis()));
    bw.addMutation(m);
    bw.close();

    refs.updateReplicationEntries(conn, wals);

    Scanner s = conn.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    Entry<Key,Value> entry = Iterables.getOnlyElement(s);
    Status status = Status.parseFrom(entry.getValue().get());
    Assert.assertTrue(status.getClosed());
  }

  @Test
  public void usedWalsAreNotClosed() throws Exception {
    String file = "file:/accumulo/wal/tserver+port/12345";
    Set<String> wals = Collections.singleton(file);
    Instance inst = new MockInstance(testName.getMethodName());
    Connector conn = inst.getConnector("root", new PasswordToken(""));

    BatchWriter bw = conn.createBatchWriter(MetadataTable.NAME, new BatchWriterConfig());
    Mutation m = new Mutation(ReplicationSection.getRowPrefix() + file);
    m.put(ReplicationSection.COLF, new Text("1"), StatusUtil.fileCreatedValue(System.currentTimeMillis()));
    bw.addMutation(m);
    bw.close();

    refs.updateReplicationEntries(conn, wals);

    Scanner s = conn.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    Entry<Key,Value> entry = Iterables.getOnlyElement(s);
    Status status = Status.parseFrom(entry.getValue().get());
    Assert.assertFalse(status.getClosed());
  }

  @Test
  public void partiallyReplicatedReferencedWalsAreNotClosed() throws Exception {
    String file = "file:/accumulo/wal/tserver+port/12345";
    Set<String> wals = Collections.singleton(file);
    Instance inst = new MockInstance(testName.getMethodName());
    Connector conn = inst.getConnector("root", new PasswordToken(""));

    BatchWriter bw = ReplicationTable.getBatchWriter(conn);
    Mutation m = new Mutation(file);
    StatusSection.add(m, new Text("1"), ProtobufUtil.toValue(StatusUtil.ingestedUntil(1000)));
    bw.addMutation(m);
    bw.close();

    refs.updateReplicationEntries(conn, wals);

    Scanner s = ReplicationTable.getScanner(conn);
    Entry<Key,Value> entry = Iterables.getOnlyElement(s);
    Status status = Status.parseFrom(entry.getValue().get());
    Assert.assertFalse(status.getClosed());
  }

  @Test
  public void getActiveWals() throws Exception {
    CloseWriteAheadLogReferences closeWals = EasyMock.createMockBuilder(CloseWriteAheadLogReferences.class).addMockedMethod("getActiveTservers")
        .addMockedMethod("getActiveWalsForServer").createMock();
    TInfo tinfo = EasyMock.createMock(TInfo.class);

    List<String> tservers = Arrays.asList("localhost:12345", "localhost:12346");
    EasyMock.expect(closeWals.getActiveTservers(tinfo)).andReturn(tservers);
    int numWals = 0;
    for (String tserver : tservers) {
      EasyMock.expect(closeWals.getActiveWalsForServer(tinfo, HostAndPort.fromString(tserver))).andReturn(Arrays.asList("/wal" + numWals));
      numWals++;
    }

    EasyMock.replay(closeWals);

    Set<String> wals = closeWals.getActiveWals(tinfo);

    EasyMock.verify(closeWals);

    Set<String> expectedWals = new HashSet<String>();
    for (int i = 0; i < numWals; i++) {
      expectedWals.add("/wal" + i);
    }

    Assert.assertEquals(expectedWals, wals);
  }

  @Test
  public void offlineMaster() throws Exception {
    CloseWriteAheadLogReferences closeWals = EasyMock.createMockBuilder(CloseWriteAheadLogReferences.class).addMockedMethod("getActiveTservers")
        .addMockedMethod("getActiveWalsForServer").createMock();
    TInfo tinfo = EasyMock.createMock(TInfo.class);

    EasyMock.expect(closeWals.getActiveTservers(tinfo)).andReturn(null);

    EasyMock.replay(closeWals);

    Set<String> wals = closeWals.getActiveWals(tinfo);

    EasyMock.verify(closeWals);

    Assert.assertNull("Expected to get null for active WALs", wals);
  }

  @Test
  public void offlineTserver() throws Exception {
    CloseWriteAheadLogReferences closeWals = EasyMock.createMockBuilder(CloseWriteAheadLogReferences.class).addMockedMethod("getActiveTservers")
        .addMockedMethod("getActiveWalsForServer").createMock();
    TInfo tinfo = EasyMock.createMock(TInfo.class);

    List<String> tservers = Arrays.asList("localhost:12345", "localhost:12346");
    EasyMock.expect(closeWals.getActiveTservers(tinfo)).andReturn(tservers);
    EasyMock.expect(closeWals.getActiveWalsForServer(tinfo, HostAndPort.fromString("localhost:12345"))).andReturn(Arrays.asList("/wal" + 0));
    EasyMock.expect(closeWals.getActiveWalsForServer(tinfo, HostAndPort.fromString("localhost:12346"))).andReturn(null);

    EasyMock.replay(closeWals);

    Set<String> wals = closeWals.getActiveWals(tinfo);

    EasyMock.verify(closeWals);

    Assert.assertNull("Expected to get null for active WALs", wals);
  }
}
