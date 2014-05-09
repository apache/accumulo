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
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.LogColumnFamily;
import org.apache.accumulo.core.protobuf.ProtobufUtil;
import org.apache.accumulo.core.replication.ReplicationSchema.StatusSection;
import org.apache.accumulo.core.replication.StatusUtil;
import org.apache.accumulo.core.replication.proto.Replication.Status;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.server.replication.ReplicationTable;
import org.apache.hadoop.io.Text;
import org.easymock.IAnswer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class CloseWriteAheadLogReferencesTest {

  private CloseWriteAheadLogReferences refs;
  private Credentials creds;
  private Instance inst;

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setup() {
    creds = createMock(Credentials.class);
    inst = createMock(Instance.class);
    refs = new CloseWriteAheadLogReferences(inst, creds);
  }

  @Test
  public void findOneWalFromMetadata() throws Exception {
    Connector conn = createMock(Connector.class);
    BatchScanner bs = createMock(BatchScanner.class);

    // Fake out some data
    final ArrayList<Entry<Key,Value>> data = new ArrayList<>();
    LogEntry logEntry = new LogEntry();
    logEntry.extent = new KeyExtent(new Text("1"), new Text("b"), new Text("a"));
    logEntry.filename = "hdfs://localhost:8020/accumulo/wal/tserver+port/" + UUID.randomUUID();
    logEntry.server = "tserver1";
    logEntry.tabletId = 1;
    logEntry.logSet = Collections.singleton(logEntry.filename);
    data.add(Maps.immutableEntry(new Key(logEntry.getRow(), logEntry.getColumnFamily(), logEntry.getColumnQualifier()), new Value(logEntry.getValue())));

    // Get a batchscanner, scan the tablets section, fetch only the logs
    expect(conn.createBatchScanner(MetadataTable.NAME, new Authorizations(), 4)).andReturn(bs);
    bs.setRanges(Collections.singleton(TabletsSection.getRange()));
    expectLastCall().once();
    bs.fetchColumnFamily(LogColumnFamily.NAME);
    expectLastCall().once();
    expect(bs.iterator()).andAnswer(new IAnswer<Iterator<Entry<Key,Value>>>() {

      @Override
      public Iterator<Entry<Key,Value>> answer() throws Throwable {
        return data.iterator();
      }

    });
    // Close the bs
    bs.close();
    expectLastCall().once();

    replay(conn, bs);

    // Validate
    Set<String> wals = refs.getReferencedWals(conn);
    Assert.assertEquals(Collections.singleton(logEntry.filename), wals);

    verify(conn, bs);
  }

  @Test
  public void findManyWalFromSingleMetadata() throws Exception {
    Connector conn = createMock(Connector.class);
    BatchScanner bs = createMock(BatchScanner.class);

    // Fake out some data
    final ArrayList<Entry<Key,Value>> data = new ArrayList<>();
    LogEntry logEntry = new LogEntry();
    logEntry.extent = new KeyExtent(new Text("1"), new Text("b"), new Text("a"));
    logEntry.filename = "hdfs://localhost:8020/accumulo/wal/tserver+port/" + UUID.randomUUID();
    logEntry.server = "tserver1";
    logEntry.tabletId = 1;
    // Multiple DFSLoggers
    logEntry.logSet = Sets.newHashSet(logEntry.filename, "hdfs://localhost:8020/accumulo/wal/tserver+port/" + UUID.randomUUID());
    data.add(Maps.immutableEntry(new Key(logEntry.getRow(), logEntry.getColumnFamily(), logEntry.getColumnQualifier()), new Value(logEntry.getValue())));

    // Get a batchscanner, scan the tablets section, fetch only the logs
    expect(conn.createBatchScanner(MetadataTable.NAME, new Authorizations(), 4)).andReturn(bs);
    bs.setRanges(Collections.singleton(TabletsSection.getRange()));
    expectLastCall().once();
    bs.fetchColumnFamily(LogColumnFamily.NAME);
    expectLastCall().once();
    expect(bs.iterator()).andAnswer(new IAnswer<Iterator<Entry<Key,Value>>>() {

      @Override
      public Iterator<Entry<Key,Value>> answer() throws Throwable {
        return data.iterator();
      }

    });
    // Close the bs
    bs.close();
    expectLastCall().once();

    replay(conn, bs);

    // Validate
    Set<String> wals = refs.getReferencedWals(conn);
    Assert.assertEquals(logEntry.logSet, wals);

    verify(conn, bs);
  }

  @Test
  public void findManyRefsToSingleWalFromMetadata() throws Exception {
    Connector conn = createMock(Connector.class);
    BatchScanner bs = createMock(BatchScanner.class);

    String uuid = UUID.randomUUID().toString();

    // Fake out some data
    final ArrayList<Entry<Key,Value>> data = new ArrayList<>();
    LogEntry logEntry = new LogEntry();
    logEntry.extent = new KeyExtent(new Text("1"), new Text("b"), new Text("a"));
    logEntry.filename = "hdfs://localhost:8020/accumulo/wal/tserver+port/" + uuid;
    logEntry.server = "tserver1";
    logEntry.tabletId = 1;
    logEntry.logSet = Collections.singleton(logEntry.filename);
    data.add(Maps.immutableEntry(new Key(logEntry.getRow(), logEntry.getColumnFamily(), logEntry.getColumnQualifier()), new Value(logEntry.getValue())));
    
    logEntry.extent = new KeyExtent(new Text("1"), new Text("c"), new Text("b"));
    logEntry.server = "tserver1";
    logEntry.tabletId = 2;
    logEntry.logSet = Collections.singleton(logEntry.filename);
    data.add(Maps.immutableEntry(new Key(logEntry.getRow(), logEntry.getColumnFamily(), logEntry.getColumnQualifier()), new Value(logEntry.getValue())));
    
    logEntry.extent = new KeyExtent(new Text("1"), null, new Text("c"));
    logEntry.server = "tserver1";
    logEntry.tabletId = 3;
    logEntry.logSet = Collections.singleton(logEntry.filename);
    data.add(Maps.immutableEntry(new Key(logEntry.getRow(), logEntry.getColumnFamily(), logEntry.getColumnQualifier()), new Value(logEntry.getValue())));

    // Get a batchscanner, scan the tablets section, fetch only the logs
    expect(conn.createBatchScanner(MetadataTable.NAME, new Authorizations(), 4)).andReturn(bs);
    bs.setRanges(Collections.singleton(TabletsSection.getRange()));
    expectLastCall().once();
    bs.fetchColumnFamily(LogColumnFamily.NAME);
    expectLastCall().once();
    expect(bs.iterator()).andAnswer(new IAnswer<Iterator<Entry<Key,Value>>>() {

      @Override
      public Iterator<Entry<Key,Value>> answer() throws Throwable {
        return data.iterator();
      }

    });
    // Close the bs
    bs.close();
    expectLastCall().once();

    replay(conn, bs);

    // Validate
    Set<String> wals = refs.getReferencedWals(conn);
    Assert.assertEquals(Collections.singleton(logEntry.filename), wals);

    verify(conn, bs);
  }

  @Test
  public void findRefsToManyWalsFromMetadata() throws Exception {
    Connector conn = createMock(Connector.class);
    BatchScanner bs = createMock(BatchScanner.class);

    String file1 = "hdfs://localhost:8020/accumulo/wal/tserver1+port/" + UUID.randomUUID(),
        file2 = "hdfs://localhost:8020/accumulo/wal/tserver2+port/" + UUID.randomUUID(),
        file3 = "hdfs://localhost:8020/accumulo/wal/tserver3+port/" + UUID.randomUUID();

    // Fake out some data
    final ArrayList<Entry<Key,Value>> data = new ArrayList<>();
    LogEntry logEntry = new LogEntry();
    logEntry.extent = new KeyExtent(new Text("1"), new Text("b"), new Text("a"));
    logEntry.filename = file1;
    logEntry.server = "tserver1";
    logEntry.tabletId = 1;
    logEntry.logSet = Collections.singleton(logEntry.filename);
    data.add(Maps.immutableEntry(new Key(logEntry.getRow(), logEntry.getColumnFamily(), logEntry.getColumnQualifier()), new Value(logEntry.getValue())));

    logEntry.extent = new KeyExtent(new Text("5"), null, null);
    logEntry.tabletId = 2;
    data.add(Maps.immutableEntry(new Key(logEntry.getRow(), logEntry.getColumnFamily(), logEntry.getColumnQualifier()), new Value(logEntry.getValue())));
    
    logEntry.extent = new KeyExtent(new Text("3"), new Text("b"), new Text("a"));
    logEntry.filename = file2;
    logEntry.server = "tserver2";
    logEntry.tabletId = 3;
    logEntry.logSet = Collections.singleton(logEntry.filename);
    data.add(Maps.immutableEntry(new Key(logEntry.getRow(), logEntry.getColumnFamily(), logEntry.getColumnQualifier()), new Value(logEntry.getValue())));
    
    logEntry.extent = new KeyExtent(new Text("3"), new Text("c"), new Text("b"));
    logEntry.tabletId = 4;
    logEntry.logSet = Collections.singleton(logEntry.filename);
    data.add(Maps.immutableEntry(new Key(logEntry.getRow(), logEntry.getColumnFamily(), logEntry.getColumnQualifier()), new Value(logEntry.getValue())));
    
    logEntry.extent = new KeyExtent(new Text("4"), new Text("5"), new Text("0"));
    logEntry.filename = file3;
    logEntry.server = "tserver3";
    logEntry.tabletId = 5;
    logEntry.logSet = Collections.singleton(logEntry.filename);
    data.add(Maps.immutableEntry(new Key(logEntry.getRow(), logEntry.getColumnFamily(), logEntry.getColumnQualifier()), new Value(logEntry.getValue())));
    
    logEntry.extent = new KeyExtent(new Text("4"), new Text("8"), new Text("5"));
    logEntry.server = "tserver3";
    logEntry.tabletId = 7;
    logEntry.logSet = Collections.singleton(logEntry.filename);
    data.add(Maps.immutableEntry(new Key(logEntry.getRow(), logEntry.getColumnFamily(), logEntry.getColumnQualifier()), new Value(logEntry.getValue())));
    
    logEntry.extent = new KeyExtent(new Text("4"), null, new Text("8"));
    logEntry.server = "tserver3";
    logEntry.tabletId = 15;
    logEntry.logSet = Collections.singleton(logEntry.filename);
    data.add(Maps.immutableEntry(new Key(logEntry.getRow(), logEntry.getColumnFamily(), logEntry.getColumnQualifier()), new Value(logEntry.getValue())));

    // Get a batchscanner, scan the tablets section, fetch only the logs
    expect(conn.createBatchScanner(MetadataTable.NAME, new Authorizations(), 4)).andReturn(bs);
    bs.setRanges(Collections.singleton(TabletsSection.getRange()));
    expectLastCall().once();
    bs.fetchColumnFamily(LogColumnFamily.NAME);
    expectLastCall().once();
    expect(bs.iterator()).andAnswer(new IAnswer<Iterator<Entry<Key,Value>>>() {

      @Override
      public Iterator<Entry<Key,Value>> answer() throws Throwable {
        return data.iterator();
      }

    });
    // Close the bs
    bs.close();
    expectLastCall().once();

    replay(conn, bs);

    // Validate
    Set<String> wals = refs.getReferencedWals(conn);
    Assert.assertEquals(Sets.newHashSet(file1, file2, file3), wals);

    verify(conn, bs);
  }

  @Test
  public void unusedWalsAreClosed() throws Exception {
    Set<String> wals = Collections.emptySet();
    Instance inst = new MockInstance(testName.getMethodName());
    Connector conn = inst.getConnector("root", new PasswordToken(""));

    ReplicationTable.create(conn);
    BatchWriter bw = conn.createBatchWriter(ReplicationTable.NAME, new BatchWriterConfig());
    Mutation m = new Mutation("file:/accumulo/wal/tserver+port/12345");
    StatusSection.add(m, new Text("1"), StatusUtil.newFileValue());
    bw.addMutation(m);
    bw.close();

    refs.updateReplicationEntries(conn, wals);

    Scanner s = ReplicationTable.getScanner(conn);
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

    ReplicationTable.create(conn);
    BatchWriter bw = conn.createBatchWriter(ReplicationTable.NAME, new BatchWriterConfig());
    Mutation m = new Mutation(file);
    StatusSection.add(m, new Text("1"), StatusUtil.newFileValue());
    bw.addMutation(m);
    bw.close();

    refs.updateReplicationEntries(conn, wals);

    Scanner s = ReplicationTable.getScanner(conn);
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

    ReplicationTable.create(conn);
    BatchWriter bw = conn.createBatchWriter(ReplicationTable.NAME, new BatchWriterConfig());
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

}
