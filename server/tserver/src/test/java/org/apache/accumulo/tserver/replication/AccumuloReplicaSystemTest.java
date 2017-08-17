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
package org.apache.accumulo.tserver.replication;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.replication.ReplicationTarget;
import org.apache.accumulo.core.replication.thrift.ReplicationServicer.Client;
import org.apache.accumulo.core.replication.thrift.WalEdits;
import org.apache.accumulo.core.security.thrift.TCredentials;
import org.apache.accumulo.server.data.ServerMutation;
import org.apache.accumulo.server.replication.proto.Replication.Status;
import org.apache.accumulo.tserver.logger.LogEvents;
import org.apache.accumulo.tserver.logger.LogFileKey;
import org.apache.accumulo.tserver.logger.LogFileValue;
import org.apache.accumulo.tserver.replication.AccumuloReplicaSystem.ReplicationStats;
import org.apache.accumulo.tserver.replication.AccumuloReplicaSystem.WalClientExecReturn;
import org.apache.accumulo.tserver.replication.AccumuloReplicaSystem.WalReplication;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

public class AccumuloReplicaSystemTest {

  @Test
  public void onlyChooseMutationsForDesiredTableWithOpenStatus() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);

    LogFileKey key = new LogFileKey();
    LogFileValue value = new LogFileValue();

    // What is seq used for?
    key.seq = 1l;

    /*
     * Disclaimer: the following series of LogFileKey and LogFileValue pairs have *no* bearing whatsoever in reality regarding what these entries would actually
     * look like in a WAL. They are solely for testing that each LogEvents is handled, order is not important.
     */
    key.event = LogEvents.DEFINE_TABLET;
    key.tablet = new KeyExtent(Table.ID.of("1"), null, null);
    key.tid = 1;

    key.write(dos);
    value.write(dos);

    key.tablet = null;
    key.event = LogEvents.MUTATION;
    key.filename = "/accumulo/wals/tserver+port/" + UUID.randomUUID();
    value.mutations = Arrays.<Mutation> asList(new ServerMutation(new Text("row")));

    key.write(dos);
    value.write(dos);

    key.event = LogEvents.DEFINE_TABLET;
    key.tablet = new KeyExtent(Table.ID.of("2"), null, null);
    key.tid = 2;
    value.mutations = Collections.emptyList();

    key.write(dos);
    value.write(dos);

    key.event = LogEvents.OPEN;
    key.tid = LogFileKey.VERSION;
    key.tserverSession = "foobar";

    key.write(dos);
    value.write(dos);

    key.tablet = null;
    key.event = LogEvents.MUTATION;
    key.filename = "/accumulo/wals/tserver+port/" + UUID.randomUUID();
    value.mutations = Arrays.<Mutation> asList(new ServerMutation(new Text("badrow")));

    key.write(dos);
    value.write(dos);

    key.event = LogEvents.COMPACTION_START;
    key.tid = 2;
    key.filename = "/accumulo/tables/1/t-000001/A000001.rf";
    value.mutations = Collections.emptyList();

    key.write(dos);
    value.write(dos);

    key.event = LogEvents.DEFINE_TABLET;
    key.tablet = new KeyExtent(Table.ID.of("1"), null, null);
    key.tid = 3;
    value.mutations = Collections.emptyList();

    key.write(dos);
    value.write(dos);

    key.event = LogEvents.COMPACTION_FINISH;
    key.tid = 6;
    value.mutations = Collections.emptyList();

    key.write(dos);
    value.write(dos);

    key.tablet = null;
    key.event = LogEvents.MUTATION;
    key.tid = 3;
    key.filename = "/accumulo/wals/tserver+port/" + UUID.randomUUID();
    value.mutations = Arrays.<Mutation> asList(new ServerMutation(new Text("row")));

    key.write(dos);
    value.write(dos);

    dos.close();

    Map<String,String> confMap = new HashMap<>();
    confMap.put(Property.REPLICATION_NAME.getKey(), "source");
    AccumuloConfiguration conf = new ConfigurationCopy(confMap);

    AccumuloReplicaSystem ars = new AccumuloReplicaSystem();
    ars.setConf(conf);

    Status status = Status.newBuilder().setBegin(0).setEnd(0).setInfiniteEnd(true).setClosed(false).build();
    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
    WalReplication repl = ars.getWalEdits(new ReplicationTarget("peer", "1", Table.ID.of("1")), dis, new Path("/accumulo/wals/tserver+port/wal"), status,
        Long.MAX_VALUE, new HashSet<Integer>());

    // We stopped because we got to the end of the file
    Assert.assertEquals(9, repl.entriesConsumed);
    Assert.assertEquals(2, repl.walEdits.getEditsSize());
    Assert.assertEquals(2, repl.sizeInRecords);
    Assert.assertNotEquals(0, repl.sizeInBytes);
  }

  @Test
  public void onlyChooseMutationsForDesiredTableWithClosedStatus() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);

    LogFileKey key = new LogFileKey();
    LogFileValue value = new LogFileValue();

    // What is seq used for?
    key.seq = 1l;

    /*
     * Disclaimer: the following series of LogFileKey and LogFileValue pairs have *no* bearing whatsoever in reality regarding what these entries would actually
     * look like in a WAL. They are solely for testing that each LogEvents is handled, order is not important.
     */
    key.event = LogEvents.DEFINE_TABLET;
    key.tablet = new KeyExtent(Table.ID.of("1"), null, null);
    key.tid = 1;

    key.write(dos);
    value.write(dos);

    key.tablet = null;
    key.event = LogEvents.MUTATION;
    key.filename = "/accumulo/wals/tserver+port/" + UUID.randomUUID();
    value.mutations = Arrays.<Mutation> asList(new ServerMutation(new Text("row")));

    key.write(dos);
    value.write(dos);

    key.event = LogEvents.DEFINE_TABLET;
    key.tablet = new KeyExtent(Table.ID.of("2"), null, null);
    key.tid = 2;
    value.mutations = Collections.emptyList();

    key.write(dos);
    value.write(dos);

    key.event = LogEvents.OPEN;
    key.tid = LogFileKey.VERSION;
    key.tserverSession = "foobar";

    key.write(dos);
    value.write(dos);

    key.tablet = null;
    key.event = LogEvents.MUTATION;
    key.filename = "/accumulo/wals/tserver+port/" + UUID.randomUUID();
    value.mutations = Arrays.<Mutation> asList(new ServerMutation(new Text("badrow")));

    key.write(dos);
    value.write(dos);

    key.event = LogEvents.COMPACTION_START;
    key.tid = 2;
    key.filename = "/accumulo/tables/1/t-000001/A000001.rf";
    value.mutations = Collections.emptyList();

    key.write(dos);
    value.write(dos);

    key.event = LogEvents.DEFINE_TABLET;
    key.tablet = new KeyExtent(Table.ID.of("1"), null, null);
    key.tid = 3;
    value.mutations = Collections.emptyList();

    key.write(dos);
    value.write(dos);

    key.event = LogEvents.COMPACTION_FINISH;
    key.tid = 6;
    value.mutations = Collections.emptyList();

    key.write(dos);
    value.write(dos);

    key.tablet = null;
    key.event = LogEvents.MUTATION;
    key.tid = 3;
    key.filename = "/accumulo/wals/tserver+port/" + UUID.randomUUID();
    value.mutations = Arrays.<Mutation> asList(new ServerMutation(new Text("row")));

    key.write(dos);
    value.write(dos);

    dos.close();

    Map<String,String> confMap = new HashMap<>();
    confMap.put(Property.REPLICATION_NAME.getKey(), "source");
    AccumuloConfiguration conf = new ConfigurationCopy(confMap);

    AccumuloReplicaSystem ars = new AccumuloReplicaSystem();
    ars.setConf(conf);

    // Setting the file to be closed with the infinite end implies that we need to bump the begin up to Long.MAX_VALUE
    // If it were still open, more data could be appended that we need to process
    Status status = Status.newBuilder().setBegin(0).setEnd(0).setInfiniteEnd(true).setClosed(true).build();
    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
    WalReplication repl = ars.getWalEdits(new ReplicationTarget("peer", "1", Table.ID.of("1")), dis, new Path("/accumulo/wals/tserver+port/wal"), status,
        Long.MAX_VALUE, new HashSet<Integer>());

    // We stopped because we got to the end of the file
    Assert.assertEquals(Long.MAX_VALUE, repl.entriesConsumed);
    Assert.assertEquals(2, repl.walEdits.getEditsSize());
    Assert.assertEquals(2, repl.sizeInRecords);
    Assert.assertNotEquals(0, repl.sizeInBytes);
  }

  @Test
  public void mutationsNotReReplicatedToPeers() throws Exception {
    AccumuloReplicaSystem ars = new AccumuloReplicaSystem();
    Map<String,String> confMap = new HashMap<>();
    confMap.put(Property.REPLICATION_NAME.getKey(), "source");
    AccumuloConfiguration conf = new ConfigurationCopy(confMap);

    ars.setConf(conf);

    LogFileValue value = new LogFileValue();
    value.mutations = new ArrayList<>();

    Mutation m = new Mutation("row");
    m.put("", "", new Value(new byte[0]));
    value.mutations.add(m);

    m = new Mutation("row2");
    m.put("", "", new Value(new byte[0]));
    m.addReplicationSource("peer");
    value.mutations.add(m);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(baos);

    // Replicate our 2 mutations to "peer", from tableid 1 to tableid 1
    ars.writeValueAvoidingReplicationCycles(out, value, new ReplicationTarget("peer", "1", Table.ID.of("1")));

    out.close();

    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    DataInputStream in = new DataInputStream(bais);

    int numMutations = in.readInt();
    Assert.assertEquals(1, numMutations);

    m = new Mutation();
    m.readFields(in);

    Assert.assertEquals("row", new String(m.getRow()));
    Assert.assertEquals(1, m.getReplicationSources().size());
    Assert.assertTrue("Expected source cluster to be listed in mutation replication source", m.getReplicationSources().contains("source"));
  }

  @Test
  public void endOfFileExceptionOnClosedWalImpliesFullyReplicated() throws Exception {
    Map<String,String> confMap = new HashMap<>();
    confMap.put(Property.REPLICATION_NAME.getKey(), "source");
    AccumuloConfiguration conf = new ConfigurationCopy(confMap);

    AccumuloReplicaSystem ars = new AccumuloReplicaSystem();
    ars.setConf(conf);

    // Setting the file to be closed with the infinite end implies that we need to bump the begin up to Long.MAX_VALUE
    // If it were still open, more data could be appended that we need to process
    Status status = Status.newBuilder().setBegin(100).setEnd(0).setInfiniteEnd(true).setClosed(true).build();
    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(new byte[0]));
    WalReplication repl = ars.getWalEdits(new ReplicationTarget("peer", "1", Table.ID.of("1")), dis, new Path("/accumulo/wals/tserver+port/wal"), status,
        Long.MAX_VALUE, new HashSet<Integer>());

    // We stopped because we got to the end of the file
    Assert.assertEquals(Long.MAX_VALUE, repl.entriesConsumed);
    Assert.assertEquals(0, repl.walEdits.getEditsSize());
    Assert.assertEquals(0, repl.sizeInRecords);
    Assert.assertEquals(0, repl.sizeInBytes);
  }

  @Test
  public void endOfFileExceptionOnOpenWalImpliesMoreReplication() throws Exception {
    Map<String,String> confMap = new HashMap<>();
    confMap.put(Property.REPLICATION_NAME.getKey(), "source");
    AccumuloConfiguration conf = new ConfigurationCopy(confMap);

    AccumuloReplicaSystem ars = new AccumuloReplicaSystem();
    ars.setConf(conf);

    // Setting the file to be closed with the infinite end implies that we need to bump the begin up to Long.MAX_VALUE
    // If it were still open, more data could be appended that we need to process
    Status status = Status.newBuilder().setBegin(100).setEnd(0).setInfiniteEnd(true).setClosed(false).build();
    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(new byte[0]));
    WalReplication repl = ars.getWalEdits(new ReplicationTarget("peer", "1", Table.ID.of("1")), dis, new Path("/accumulo/wals/tserver+port/wal"), status,
        Long.MAX_VALUE, new HashSet<Integer>());

    // We stopped because we got to the end of the file
    Assert.assertEquals(0, repl.entriesConsumed);
    Assert.assertEquals(0, repl.walEdits.getEditsSize());
    Assert.assertEquals(0, repl.sizeInRecords);
    Assert.assertEquals(0, repl.sizeInBytes);
  }

  @Test
  public void restartInFileKnowsAboutPreviousTableDefines() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);

    LogFileKey key = new LogFileKey();
    LogFileValue value = new LogFileValue();

    // What is seq used for?
    key.seq = 1l;

    /*
     * Disclaimer: the following series of LogFileKey and LogFileValue pairs have *no* bearing whatsoever in reality regarding what these entries would actually
     * look like in a WAL. They are solely for testing that each LogEvents is handled, order is not important.
     */
    key.event = LogEvents.DEFINE_TABLET;
    key.tablet = new KeyExtent(Table.ID.of("1"), null, null);
    key.tid = 1;

    key.write(dos);
    value.write(dos);

    key.tablet = null;
    key.event = LogEvents.MUTATION;
    key.filename = "/accumulo/wals/tserver+port/" + UUID.randomUUID();
    value.mutations = Arrays.<Mutation> asList(new ServerMutation(new Text("row")));

    key.write(dos);
    value.write(dos);

    key.tablet = null;
    key.event = LogEvents.MUTATION;
    key.tid = 1;
    key.filename = "/accumulo/wals/tserver+port/" + UUID.randomUUID();
    value.mutations = Arrays.<Mutation> asList(new ServerMutation(new Text("row")));

    key.write(dos);
    value.write(dos);

    dos.close();

    Map<String,String> confMap = new HashMap<>();
    confMap.put(Property.REPLICATION_NAME.getKey(), "source");
    AccumuloConfiguration conf = new ConfigurationCopy(confMap);

    AccumuloReplicaSystem ars = new AccumuloReplicaSystem();
    ars.setConf(conf);

    Status status = Status.newBuilder().setBegin(0).setEnd(0).setInfiniteEnd(true).setClosed(false).build();
    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));

    HashSet<Integer> tids = new HashSet<>();

    // Only consume the first mutation, not the second
    WalReplication repl = ars.getWalEdits(new ReplicationTarget("peer", "1", Table.ID.of("1")), dis, new Path("/accumulo/wals/tserver+port/wal"), status, 1l,
        tids);

    // We stopped because we got to the end of the file
    Assert.assertEquals(2, repl.entriesConsumed);
    Assert.assertEquals(1, repl.walEdits.getEditsSize());
    Assert.assertEquals(1, repl.sizeInRecords);
    Assert.assertNotEquals(0, repl.sizeInBytes);

    status = Status.newBuilder(status).setBegin(2).build();

    // Consume the rest of the mutations
    repl = ars.getWalEdits(new ReplicationTarget("peer", "1", Table.ID.of("1")), dis, new Path("/accumulo/wals/tserver+port/wal"), status, 1l, tids);

    // We stopped because we got to the end of the file
    Assert.assertEquals(1, repl.entriesConsumed);
    Assert.assertEquals(1, repl.walEdits.getEditsSize());
    Assert.assertEquals(1, repl.sizeInRecords);
    Assert.assertNotEquals(0, repl.sizeInBytes);
  }

  @Test
  public void dontSendEmptyDataToPeer() throws Exception {
    Client replClient = createMock(Client.class);
    AccumuloReplicaSystem ars = createMock(AccumuloReplicaSystem.class);
    WalEdits edits = new WalEdits(Collections.<ByteBuffer> emptyList());
    WalReplication walReplication = new WalReplication(edits, 0, 0, 0);

    ReplicationTarget target = new ReplicationTarget("peer", "2", Table.ID.of("1"));
    DataInputStream input = null;
    Path p = new Path("/accumulo/wals/tserver+port/" + UUID.randomUUID().toString());
    Status status = null;
    long sizeLimit = Long.MAX_VALUE;
    String remoteTableId = target.getRemoteIdentifier();
    TCredentials tcreds = null;
    Set<Integer> tids = new HashSet<>();

    WalClientExecReturn walClientExec = ars.new WalClientExecReturn(target, input, p, status, sizeLimit, remoteTableId, tcreds, tids);

    expect(ars.getWalEdits(target, input, p, status, sizeLimit, tids)).andReturn(walReplication);

    replay(replClient, ars);

    ReplicationStats stats = walClientExec.execute(replClient);

    verify(replClient, ars);

    Assert.assertEquals(new ReplicationStats(0l, 0l, 0l), stats);
  }

  @Test
  public void consumedButNotSentDataShouldBeRecorded() throws Exception {
    Client replClient = createMock(Client.class);
    AccumuloReplicaSystem ars = createMock(AccumuloReplicaSystem.class);
    WalEdits edits = new WalEdits(Collections.<ByteBuffer> emptyList());
    WalReplication walReplication = new WalReplication(edits, 0, 5, 0);

    ReplicationTarget target = new ReplicationTarget("peer", "2", Table.ID.of("1"));
    DataInputStream input = null;
    Path p = new Path("/accumulo/wals/tserver+port/" + UUID.randomUUID().toString());
    Status status = null;
    long sizeLimit = Long.MAX_VALUE;
    String remoteTableId = target.getRemoteIdentifier();
    TCredentials tcreds = null;
    Set<Integer> tids = new HashSet<>();

    WalClientExecReturn walClientExec = ars.new WalClientExecReturn(target, input, p, status, sizeLimit, remoteTableId, tcreds, tids);

    expect(ars.getWalEdits(target, input, p, status, sizeLimit, tids)).andReturn(walReplication);

    replay(replClient, ars);

    ReplicationStats stats = walClientExec.execute(replClient);

    verify(replClient, ars);

    Assert.assertEquals(new ReplicationStats(0l, 0l, 5l), stats);
  }

  @Test
  public void testUserPassword() throws Exception {
    AccumuloReplicaSystem ars = new AccumuloReplicaSystem();
    ReplicationTarget target = new ReplicationTarget("peer", "peer_table", Table.ID.of("1"));
    String user = "user", password = "password";

    Map<String,String> confMap = new HashMap<>();
    confMap.put(Property.REPLICATION_PEER_USER.getKey() + target.getPeerName(), user);
    confMap.put(Property.REPLICATION_PEER_PASSWORD.getKey() + target.getPeerName(), password);
    AccumuloConfiguration conf = new ConfigurationCopy(confMap);

    assertEquals(user, ars.getPrincipal(conf, target));
    assertEquals(password, ars.getPassword(conf, target));
  }

  @Test
  public void testUserKeytab() throws Exception {
    AccumuloReplicaSystem ars = new AccumuloReplicaSystem();
    ReplicationTarget target = new ReplicationTarget("peer", "peer_table", Table.ID.of("1"));
    String user = "user", keytab = "/etc/security/keytabs/replication.keytab";

    Map<String,String> confMap = new HashMap<>();
    confMap.put(Property.REPLICATION_PEER_USER.getKey() + target.getPeerName(), user);
    confMap.put(Property.REPLICATION_PEER_KEYTAB.getKey() + target.getPeerName(), keytab);
    AccumuloConfiguration conf = new ConfigurationCopy(confMap);

    assertEquals(user, ars.getPrincipal(conf, target));
    assertEquals(keytab, ars.getKeytab(conf, target));
  }
}
