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
package org.apache.accumulo.server.util;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ReplicationColumnFamily;
import org.apache.accumulo.core.replication.StatusUtil;
import org.apache.accumulo.core.replication.proto.Replication.Status;
import org.apache.accumulo.server.fs.FileRef;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.zookeeper.ZooLock;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Sets;

public class MasterMetadataUtilTest {

  @Test
  public void checkForReplicationEntry() throws Exception {
    KeyExtent extent = new KeyExtent(new Text("1"), new Text("b"), new Text("a"));
    FileRef path = new FileRef("a-00000/F00000.rf", new Path("file:///accumulo/tables/1/a-00000/F00000.rf"));
    // 1MB, 1k elements
    DataFileValue dfv = new DataFileValue(1048576, 1000);
    String time = "" + System.currentTimeMillis();
    Set<FileRef> filesInUseByScans = Collections.emptySet();
    String address = "127.0.0.1+9997";
    String host = "localhost:12345", wal = "file:///accumulo/wal/" + address + "/" + UUID.randomUUID();
    Set<String> unusedWalLogs = Sets.newHashSet(host+"/"+wal);
    TServerInstance lastLocation = new TServerInstance(address, 1);
    long flushId = 1;

    ZooLock lockMock = EasyMock.createNiceMock(ZooLock.class);
    EasyMock.expect(lockMock.getSessionId()).andReturn(1l).anyTimes();

    Mutation m = MasterMetadataUtil.getUpdateForTabletDataFile(extent, path, null, dfv, time, filesInUseByScans, address, lockMock, unusedWalLogs,
        lastLocation, flushId, true);
    EasyMock.replay();

    Assert.assertEquals(extent.getMetadataEntry(), new Text(m.getRow()));
    List<ColumnUpdate> updates = m.getUpdates();
    Assert.assertTrue("Expected column updates", !updates.isEmpty());

    boolean foundRepl = false;
    for (ColumnUpdate update : updates) {
      Text colfam = new Text(update.getColumnFamily());
      if (ReplicationColumnFamily.NAME.equals(colfam)) {
        Assert.assertFalse("Already found an update to " + ReplicationColumnFamily.NAME, foundRepl);
        Assert.assertEquals(new Text(wal), new Text(update.getColumnQualifier()));
        Assert.assertEquals(StatusUtil.fileClosed(), Status.parseFrom(update.getValue()));
        foundRepl = true;
      }
    }

    Assert.assertTrue("Did not find column update for " + ReplicationColumnFamily.NAME, foundRepl);
  }

  @Test
  public void checkForManyReplicationEntries() throws Exception {
    KeyExtent extent = new KeyExtent(new Text("1"), new Text("b"), new Text("a"));
    FileRef path = new FileRef("a-00000/F00000.rf", new Path("file:///accumulo/tables/1/a-00000/F00000.rf"));
    // 1MB, 1k elements
    DataFileValue dfv = new DataFileValue(1048576, 1000);
    String time = "" + System.currentTimeMillis();
    Set<FileRef> filesInUseByScans = Collections.emptySet();
    String address = "127.0.0.1+9997";
    String host = "localhost:12345", wal1 = "file:///accumulo/wal/" + address + "/" + UUID.randomUUID(),
        wal2 = "file:///accumulo/wal/" + address + "/" + UUID.randomUUID();
    Set<String> unusedWalLogs = Sets.newHashSet(host+"/"+wal1, host+"/"+wal2);
    TServerInstance lastLocation = new TServerInstance(address, 1);
    long flushId = 1;

    ZooLock lockMock = EasyMock.createNiceMock(ZooLock.class);
    EasyMock.expect(lockMock.getSessionId()).andReturn(1l).anyTimes();

    Mutation m = MasterMetadataUtil.getUpdateForTabletDataFile(extent, path, null, dfv, time, filesInUseByScans, address, lockMock, unusedWalLogs,
        lastLocation, flushId, true);
    EasyMock.replay();

    Assert.assertEquals(extent.getMetadataEntry(), new Text(m.getRow()));
    List<ColumnUpdate> updates = m.getUpdates();
    Assert.assertTrue("Expected column updates", !updates.isEmpty());

    Set<Text> foundReplWals = Sets.newHashSet();
    for (ColumnUpdate update : updates) {
      Text colfam = new Text(update.getColumnFamily());
      if (ReplicationColumnFamily.NAME.equals(colfam)) {
        foundReplWals.add(new Text(update.getColumnQualifier()));
        Assert.assertEquals(StatusUtil.fileClosed(), Status.parseFrom(update.getValue()));
      }
    }

    Assert.assertEquals(unusedWalLogs.size(), foundReplWals.size());
    for (Text replWal : foundReplWals) {
      Assert.assertTrue(replWal + " was not contained in original WAL set", unusedWalLogs.contains(host + "/" + replWal.toString()));
    }
  }

  @Test
  public void noWalsNoReplEntries() throws Exception {
    KeyExtent extent = new KeyExtent(new Text("1"), new Text("b"), new Text("a"));
    FileRef path = new FileRef("a-00000/F00000.rf", new Path("file:///accumulo/tables/1/a-00000/F00000.rf"));
    // 1MB, 1k elements
    DataFileValue dfv = new DataFileValue(1048576, 1000);
    String time = "" + System.currentTimeMillis();
    Set<FileRef> filesInUseByScans = Collections.emptySet();
    String address = "127.0.0.1+9997";
    Set<String> unusedWalLogs = Collections.emptySet();
    TServerInstance lastLocation = new TServerInstance(address, 1);
    long flushId = 1;

    ZooLock lockMock = EasyMock.createNiceMock(ZooLock.class);
    EasyMock.expect(lockMock.getSessionId()).andReturn(1l).anyTimes();

    Mutation m = MasterMetadataUtil.getUpdateForTabletDataFile(extent, path, null, dfv, time, filesInUseByScans, address, lockMock, unusedWalLogs,
        lastLocation, flushId, true);
    EasyMock.replay();

    Assert.assertEquals(extent.getMetadataEntry(), new Text(m.getRow()));
    List<ColumnUpdate> updates = m.getUpdates();
    Assert.assertTrue("Expected column updates", !updates.isEmpty());

    for (ColumnUpdate update : updates) {
      Text colfam = new Text(update.getColumnFamily());
      if (ReplicationColumnFamily.NAME.equals(colfam)) {
        Assert.fail("Did not expect to find replication entry: " + update);
      }
    }
  }

  @Test
  public void noReplicationOnMetadata() throws Exception {
    KeyExtent extent = new KeyExtent(new Text(MetadataTable.ID), new Text("b"), new Text("a"));
    FileRef path = new FileRef("a-00000/F00000.rf", new Path("file:///accumulo/tables/1/a-00000/F00000.rf"));
    // 1MB, 1k elements
    DataFileValue dfv = new DataFileValue(1048576, 1000);
    String time = "" + System.currentTimeMillis();
    Set<FileRef> filesInUseByScans = Collections.emptySet();
    String address = "127.0.0.1+9997";
    String host = "localhost:12345", wal = "file:///accumulo/wal/" + address + "/" + UUID.randomUUID();
    Set<String> unusedWalLogs = Sets.newHashSet(host+"/"+wal);
    TServerInstance lastLocation = new TServerInstance(address, 1);
    long flushId = 1;

    ZooLock lockMock = EasyMock.createNiceMock(ZooLock.class);
    EasyMock.expect(lockMock.getSessionId()).andReturn(1l).anyTimes();

    Mutation m = MasterMetadataUtil.getUpdateForTabletDataFile(extent, path, null, dfv, time, filesInUseByScans, address, lockMock, unusedWalLogs,
        lastLocation, flushId, true);
    EasyMock.replay();

    Assert.assertEquals(extent.getMetadataEntry(), new Text(m.getRow()));
    List<ColumnUpdate> updates = m.getUpdates();
    Assert.assertTrue("Expected column updates", !updates.isEmpty());

    for (ColumnUpdate update : updates) {
      Text colfam = new Text(update.getColumnFamily());
      if (ReplicationColumnFamily.NAME.equals(colfam)) {
        Assert.fail("Did not expect to find replication entry: " + update);
      }
    }
  }
}
