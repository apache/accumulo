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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.client.impl.Credentials;
import org.apache.accumulo.core.client.impl.ReplicationOperationsImpl;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.client.impl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.ReplicationSection;
import org.apache.accumulo.core.protobuf.ProtobufUtil;
import org.apache.accumulo.core.replication.ReplicationSchema.StatusSection;
import org.apache.accumulo.core.replication.ReplicationTable;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.security.thrift.TCredentials;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.core.trace.thrift.TInfo;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.master.MasterClientServiceHandler;
import org.apache.accumulo.server.replication.proto.Replication.Status;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TException;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicationOperationsImplIT extends ConfigurableMacBase {
  private static final Logger log = LoggerFactory.getLogger(ReplicationOperationsImplIT.class);

  private Instance inst;
  private Connector conn;

  @Before
  public void configureInstance() throws Exception {
    conn = getConnector();
    inst = conn.getInstance();
    ReplicationTable.setOnline(conn);
    conn.securityOperations().grantTablePermission(conn.whoami(), MetadataTable.NAME, TablePermission.WRITE);
    conn.securityOperations().grantTablePermission(conn.whoami(), ReplicationTable.NAME, TablePermission.READ);
    conn.securityOperations().grantTablePermission(conn.whoami(), ReplicationTable.NAME, TablePermission.WRITE);
  }

  /**
   * Spoof out the Master so we can call the implementation without starting a full instance.
   */
  private ReplicationOperationsImpl getReplicationOperations() throws Exception {
    Master master = EasyMock.createMock(Master.class);
    EasyMock.expect(master.getConnector()).andReturn(conn).anyTimes();
    EasyMock.expect(master.getInstance()).andReturn(inst).anyTimes();
    EasyMock.replay(master);

    final MasterClientServiceHandler mcsh = new MasterClientServiceHandler(master) {
      @Override
      protected Table.ID getTableId(Instance inst, String tableName) throws ThriftTableOperationException {
        try {
          return Table.ID.of(conn.tableOperations().tableIdMap().get(tableName));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    };

    ClientContext context = new ClientContext(inst, new Credentials("root", new PasswordToken(ROOT_PASSWORD)), getClientConfig());
    return new ReplicationOperationsImpl(context) {
      @Override
      protected boolean getMasterDrain(final TInfo tinfo, final TCredentials rpcCreds, final String tableName, final Set<String> wals)
          throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        try {
          return mcsh.drainReplicationTable(tinfo, rpcCreds, tableName, wals);
        } catch (TException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  @Test
  public void waitsUntilEntriesAreReplicated() throws Exception {
    conn.tableOperations().create("foo");
    Table.ID tableId = Table.ID.of(conn.tableOperations().tableIdMap().get("foo"));

    String file1 = "/accumulo/wals/tserver+port/" + UUID.randomUUID(), file2 = "/accumulo/wals/tserver+port/" + UUID.randomUUID();
    Status stat = Status.newBuilder().setBegin(0).setEnd(10000).setInfiniteEnd(false).setClosed(false).build();

    BatchWriter bw = ReplicationTable.getBatchWriter(conn);

    Mutation m = new Mutation(file1);
    StatusSection.add(m, tableId, ProtobufUtil.toValue(stat));
    bw.addMutation(m);

    m = new Mutation(file2);
    StatusSection.add(m, tableId, ProtobufUtil.toValue(stat));
    bw.addMutation(m);

    bw.close();

    bw = conn.createBatchWriter(MetadataTable.NAME, new BatchWriterConfig());
    m = new Mutation(ReplicationSection.getRowPrefix() + file1);
    m.put(ReplicationSection.COLF, new Text(tableId.getUtf8()), ProtobufUtil.toValue(stat));

    bw.addMutation(m);

    m = new Mutation(ReplicationSection.getRowPrefix() + file2);
    m.put(ReplicationSection.COLF, new Text(tableId.getUtf8()), ProtobufUtil.toValue(stat));

    bw.close();

    final AtomicBoolean done = new AtomicBoolean(false);
    final AtomicBoolean exception = new AtomicBoolean(false);
    final ReplicationOperationsImpl roi = getReplicationOperations();
    Thread t = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          roi.drain("foo");
        } catch (Exception e) {
          log.error("Got error", e);
          exception.set(true);
        }
        done.set(true);
      }
    });

    t.start();

    // With the records, we shouldn't be drained
    Assert.assertFalse(done.get());

    bw = conn.createBatchWriter(MetadataTable.NAME, new BatchWriterConfig());
    m = new Mutation(ReplicationSection.getRowPrefix() + file1);
    m.putDelete(ReplicationSection.COLF, new Text(tableId.getUtf8()));
    bw.addMutation(m);
    bw.flush();

    Assert.assertFalse(done.get());

    m = new Mutation(ReplicationSection.getRowPrefix() + file2);
    m.putDelete(ReplicationSection.COLF, new Text(tableId.getUtf8()));
    bw.addMutation(m);
    bw.flush();
    bw.close();

    // Removing metadata entries doesn't change anything
    Assert.assertFalse(done.get());

    // Remove the replication entries too
    bw = ReplicationTable.getBatchWriter(conn);
    m = new Mutation(file1);
    m.putDelete(StatusSection.NAME, new Text(tableId.getUtf8()));
    bw.addMutation(m);
    bw.flush();

    Assert.assertFalse(done.get());

    m = new Mutation(file2);
    m.putDelete(StatusSection.NAME, new Text(tableId.getUtf8()));
    bw.addMutation(m);
    bw.flush();

    try {
      t.join(5000);
    } catch (InterruptedException e) {
      Assert.fail("ReplicationOperations.drain did not complete");
    }

    // After both metadata and replication
    Assert.assertTrue("Drain never finished", done.get());
    Assert.assertFalse("Saw unexpectetd exception", exception.get());
  }

  @Test
  public void unrelatedReplicationRecordsDontBlockDrain() throws Exception {
    conn.tableOperations().create("foo");
    conn.tableOperations().create("bar");

    Table.ID tableId1 = Table.ID.of(conn.tableOperations().tableIdMap().get("foo"));
    Table.ID tableId2 = Table.ID.of(conn.tableOperations().tableIdMap().get("bar"));

    String file1 = "/accumulo/wals/tserver+port/" + UUID.randomUUID(), file2 = "/accumulo/wals/tserver+port/" + UUID.randomUUID();
    Status stat = Status.newBuilder().setBegin(0).setEnd(10000).setInfiniteEnd(false).setClosed(false).build();

    BatchWriter bw = ReplicationTable.getBatchWriter(conn);

    Mutation m = new Mutation(file1);
    StatusSection.add(m, tableId1, ProtobufUtil.toValue(stat));
    bw.addMutation(m);

    m = new Mutation(file2);
    StatusSection.add(m, tableId2, ProtobufUtil.toValue(stat));
    bw.addMutation(m);

    bw.close();

    bw = conn.createBatchWriter(MetadataTable.NAME, new BatchWriterConfig());
    m = new Mutation(ReplicationSection.getRowPrefix() + file1);
    m.put(ReplicationSection.COLF, new Text(tableId1.getUtf8()), ProtobufUtil.toValue(stat));

    bw.addMutation(m);

    m = new Mutation(ReplicationSection.getRowPrefix() + file2);
    m.put(ReplicationSection.COLF, new Text(tableId2.getUtf8()), ProtobufUtil.toValue(stat));

    bw.close();

    final AtomicBoolean done = new AtomicBoolean(false);
    final AtomicBoolean exception = new AtomicBoolean(false);

    final ReplicationOperationsImpl roi = getReplicationOperations();

    Thread t = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          roi.drain("foo");
        } catch (Exception e) {
          log.error("Got error", e);
          exception.set(true);
        }
        done.set(true);
      }
    });

    t.start();

    // With the records, we shouldn't be drained
    Assert.assertFalse(done.get());

    bw = conn.createBatchWriter(MetadataTable.NAME, new BatchWriterConfig());
    m = new Mutation(ReplicationSection.getRowPrefix() + file1);
    m.putDelete(ReplicationSection.COLF, new Text(tableId1.getUtf8()));
    bw.addMutation(m);
    bw.flush();

    // Removing metadata entries doesn't change anything
    Assert.assertFalse(done.get());

    // Remove the replication entries too
    bw = ReplicationTable.getBatchWriter(conn);
    m = new Mutation(file1);
    m.putDelete(StatusSection.NAME, new Text(tableId1.getUtf8()));
    bw.addMutation(m);
    bw.flush();

    try {
      t.join(5000);
    } catch (InterruptedException e) {
      Assert.fail("ReplicationOperations.drain did not complete");
    }

    // After both metadata and replication
    Assert.assertTrue("Drain never completed", done.get());
    Assert.assertFalse("Saw unexpected exception", exception.get());
  }

  @Test
  public void inprogressReplicationRecordsBlockExecution() throws Exception {
    conn.tableOperations().create("foo");

    Table.ID tableId1 = Table.ID.of(conn.tableOperations().tableIdMap().get("foo"));

    String file1 = "/accumulo/wals/tserver+port/" + UUID.randomUUID();
    Status stat = Status.newBuilder().setBegin(0).setEnd(10000).setInfiniteEnd(false).setClosed(false).build();

    BatchWriter bw = ReplicationTable.getBatchWriter(conn);

    Mutation m = new Mutation(file1);
    StatusSection.add(m, tableId1, ProtobufUtil.toValue(stat));
    bw.addMutation(m);
    bw.close();

    LogEntry logEntry = new LogEntry(new KeyExtent(tableId1, null, null), System.currentTimeMillis(), "tserver", file1);

    bw = conn.createBatchWriter(MetadataTable.NAME, new BatchWriterConfig());
    m = new Mutation(ReplicationSection.getRowPrefix() + file1);
    m.put(ReplicationSection.COLF, new Text(tableId1.getUtf8()), ProtobufUtil.toValue(stat));
    bw.addMutation(m);

    m = new Mutation(logEntry.getRow());
    m.put(logEntry.getColumnFamily(), logEntry.getColumnQualifier(), logEntry.getValue());
    bw.addMutation(m);

    bw.close();

    final AtomicBoolean done = new AtomicBoolean(false);
    final AtomicBoolean exception = new AtomicBoolean(false);
    final ReplicationOperationsImpl roi = getReplicationOperations();
    Thread t = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          roi.drain("foo");
        } catch (Exception e) {
          log.error("Got error", e);
          exception.set(true);
        }
        done.set(true);
      }
    });

    t.start();

    // With the records, we shouldn't be drained
    Assert.assertFalse(done.get());

    Status newStatus = Status.newBuilder().setBegin(1000).setEnd(2000).setInfiniteEnd(false).setClosed(true).build();
    bw = conn.createBatchWriter(MetadataTable.NAME, new BatchWriterConfig());
    m = new Mutation(ReplicationSection.getRowPrefix() + file1);
    m.put(ReplicationSection.COLF, new Text(tableId1.getUtf8()), ProtobufUtil.toValue(newStatus));
    bw.addMutation(m);
    bw.flush();

    // Removing metadata entries doesn't change anything
    Assert.assertFalse(done.get());

    // Remove the replication entries too
    bw = ReplicationTable.getBatchWriter(conn);
    m = new Mutation(file1);
    m.put(StatusSection.NAME, new Text(tableId1.getUtf8()), ProtobufUtil.toValue(newStatus));
    bw.addMutation(m);
    bw.flush();

    try {
      t.join(5000);
    } catch (InterruptedException e) {
      Assert.fail("ReplicationOperations.drain did not complete");
    }

    // New records, but not fully replicated ones don't cause it to complete
    Assert.assertFalse("Drain somehow finished", done.get());
    Assert.assertFalse("Saw unexpected exception", exception.get());
  }

  @Test
  public void laterCreatedLogsDontBlockExecution() throws Exception {
    conn.tableOperations().create("foo");

    Table.ID tableId1 = Table.ID.of(conn.tableOperations().tableIdMap().get("foo"));

    String file1 = "/accumulo/wals/tserver+port/" + UUID.randomUUID();
    Status stat = Status.newBuilder().setBegin(0).setEnd(10000).setInfiniteEnd(false).setClosed(false).build();

    BatchWriter bw = ReplicationTable.getBatchWriter(conn);
    Mutation m = new Mutation(file1);
    StatusSection.add(m, tableId1, ProtobufUtil.toValue(stat));
    bw.addMutation(m);
    bw.close();

    bw = conn.createBatchWriter(MetadataTable.NAME, new BatchWriterConfig());
    m = new Mutation(ReplicationSection.getRowPrefix() + file1);
    m.put(ReplicationSection.COLF, new Text(tableId1.getUtf8()), ProtobufUtil.toValue(stat));
    bw.addMutation(m);

    bw.close();

    log.info("Reading metadata first time");
    for (Entry<Key,Value> e : conn.createScanner(MetadataTable.NAME, Authorizations.EMPTY)) {
      log.info("{}", e.getKey());
    }

    final AtomicBoolean done = new AtomicBoolean(false);
    final AtomicBoolean exception = new AtomicBoolean(false);
    final ReplicationOperationsImpl roi = getReplicationOperations();
    Thread t = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          roi.drain("foo");
        } catch (Exception e) {
          log.error("Got error", e);
          exception.set(true);
        }
        done.set(true);
      }
    });

    t.start();

    // We need to wait long enough for the table to read once
    Thread.sleep(2000);

    // Write another file, but also delete the old files
    bw = conn.createBatchWriter(MetadataTable.NAME, new BatchWriterConfig());
    m = new Mutation(ReplicationSection.getRowPrefix() + "/accumulo/wals/tserver+port/" + UUID.randomUUID());
    m.put(ReplicationSection.COLF, new Text(tableId1.getUtf8()), ProtobufUtil.toValue(stat));
    bw.addMutation(m);
    m = new Mutation(ReplicationSection.getRowPrefix() + file1);
    m.putDelete(ReplicationSection.COLF, new Text(tableId1.getUtf8()));
    bw.addMutation(m);
    bw.close();

    log.info("Reading metadata second time");
    for (Entry<Key,Value> e : conn.createScanner(MetadataTable.NAME, Authorizations.EMPTY)) {
      log.info("{}", e.getKey());
    }

    bw = ReplicationTable.getBatchWriter(conn);
    m = new Mutation(file1);
    m.putDelete(StatusSection.NAME, new Text(tableId1.getUtf8()));
    bw.addMutation(m);
    bw.close();

    try {
      t.join(5000);
    } catch (InterruptedException e) {
      Assert.fail("ReplicationOperations.drain did not complete");
    }

    // We should pass immediately because we aren't waiting on both files to be deleted (just the one that we did)
    Assert.assertTrue("Drain didn't finish", done.get());
  }
}
