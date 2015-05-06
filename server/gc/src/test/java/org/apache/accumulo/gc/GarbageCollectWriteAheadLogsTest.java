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
package org.apache.accumulo.gc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.fate.zookeeper.ZooReader;
import org.apache.accumulo.server.AccumuloServerContext;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.master.LiveTServerSet;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.Text;
import org.easymock.EasyMock;
import org.junit.Test;

public class GarbageCollectWriteAheadLogsTest {

  private Map<TServerInstance,Set<Path>> runTest(LiveTServerSet tserverSet, String tserver, long modificationTime) throws Exception {
    // Mocks
    AccumuloServerContext context = EasyMock.createMock(AccumuloServerContext.class);
    VolumeManager fs = EasyMock.createMock(VolumeManager.class);
    final LocatedFileStatus fileStatus = EasyMock.createMock(LocatedFileStatus.class);

    // Concrete objs
    GarbageCollectWriteAheadLogs gcWals = new GarbageCollectWriteAheadLogs(context, fs, true, tserverSet);

    RemoteIterator<LocatedFileStatus> iter = new RemoteIterator<LocatedFileStatus>() {
      boolean returnedOnce = false;

      @Override
      public boolean hasNext() throws IOException {
        return !returnedOnce;
      }

      @Override
      public LocatedFileStatus next() throws IOException {
        returnedOnce = true;
        return fileStatus;
      }
    };

    Map<TServerInstance,Set<Path>> unusedLogs = new HashMap<>();

    // Path is /accumulo/wals/host+port/UUID
    Path walPath = new Path("/accumulo/wals/" + tserver + "/" + UUID.randomUUID().toString());
    EasyMock.expect(fileStatus.getPath()).andReturn(walPath).anyTimes();

    EasyMock.expect(fileStatus.getModificationTime()).andReturn(modificationTime);
    EasyMock.expect(fileStatus.isDirectory()).andReturn(false);

    EasyMock.replay(context, fs, fileStatus, tserverSet);

    gcWals.addUnusedWalsFromVolume(iter, unusedLogs, 0);

    EasyMock.verify(context, fs, fileStatus, tserverSet);

    return unusedLogs;
  }

  @Test
  public void testUnnoticedServerFailure() throws Exception {
    LiveTServerSet tserverSet = EasyMock.createMock(LiveTServerSet.class);
    TServerInstance tserverInstance = EasyMock.createMock(TServerInstance.class);
    String tserver = "host1+9997";

    // We find the TServer
    EasyMock.expect(tserverSet.find(tserver)).andReturn(tserverInstance);

    // But the modificationTime for the WAL was _way_ in the past.
    long modificationTime = 0l;

    // If the modification time for a WAL was significantly in the past (so far that the server _should_ have died
    // by now) but the GC hasn't observed via ZK Watcher that the server died, we would not treat the
    // WAL as unused.
    Map<TServerInstance,Set<Path>> unusedLogs = runTest(tserverSet, tserver, modificationTime);

    // We think the server is still alive, therefore we don't call the WAL unused.
    assertEquals(0, unusedLogs.size());
  }

  @Test
  public void testUnnoticedServerRestart() throws Exception {
    LiveTServerSet tserverSet = EasyMock.createMock(LiveTServerSet.class);
    String tserver = "host1+9997";

    // The server was _once_ alive, but we saw it died.
    // Before the LiveTServerSet gets the Watcher that it's back online (with a new session)
    // the GC runs
    EasyMock.expect(tserverSet.find(tserver)).andReturn(null);

    // Modification time for the WAL was _way_ in the past.
    long modificationTime = 0l;

    Map<TServerInstance,Set<Path>> unusedLogs = runTest(tserverSet, tserver, modificationTime);

    // If the same server comes back, it will use a new WAL, not the old one. The log should be unused
    assertEquals(1, unusedLogs.size());
  }

  @Test
  public void testAllRootLogsInZk() throws Exception {
    // Mocks
    AccumuloServerContext context = EasyMock.createMock(AccumuloServerContext.class);
    VolumeManager fs = EasyMock.createMock(VolumeManager.class);
    LiveTServerSet tserverSet = EasyMock.createMock(LiveTServerSet.class);
    Instance instance = EasyMock.createMock(Instance.class);
    ZooReader zoo = EasyMock.createMock(ZooReader.class);

    // Fake out some WAL references
    final String instanceId = UUID.randomUUID().toString();
    final List<String> currentLogs = Arrays.asList("2");
    final List<String> walogs = Arrays.asList("1");
    LogEntry currentLogEntry = new LogEntry(new KeyExtent(new Text("+r"), null, null), 2, "host1:9997", "/accumulo/wals/host1+9997/2");
    LogEntry prevLogEntry = new LogEntry(new KeyExtent(new Text("+r"), null, null), 1, "host1:9997", "/accumulo/wals/host1+9997/1");

    GarbageCollectWriteAheadLogs gcWals = new GarbageCollectWriteAheadLogs(context, fs, true, tserverSet);

    // Define the expectations
    EasyMock.expect(instance.getInstanceID()).andReturn(instanceId).anyTimes();
    EasyMock.expect(zoo.getChildren(Constants.ZROOT + "/" + instanceId + RootTable.ZROOT_TABLET_CURRENT_LOGS)).andReturn(currentLogs);
    EasyMock.expect(zoo.getChildren(Constants.ZROOT + "/" + instanceId + RootTable.ZROOT_TABLET_WALOGS)).andReturn(walogs);

    EasyMock.expect(zoo.getData(Constants.ZROOT + "/" + instanceId + RootTable.ZROOT_TABLET_CURRENT_LOGS + "/2", null)).andReturn(currentLogEntry.toBytes());
    EasyMock.expect(zoo.getData(Constants.ZROOT + "/" + instanceId + RootTable.ZROOT_TABLET_WALOGS + "/1", null)).andReturn(prevLogEntry.toBytes());

    EasyMock.replay(instance, zoo);

    // Ensure that we see logs from both current_logs and walogs
    Set<Path> rootWals = gcWals.getRootLogs(zoo, instance);

    EasyMock.verify(instance, zoo);

    assertEquals(2, rootWals.size());
    assertTrue("Expected to find WAL from walogs", rootWals.remove(new Path("/accumulo/wals/host1+9997/1")));
    assertTrue("Expected to find WAL from current_logs", rootWals.remove(new Path("/accumulo/wals/host1+9997/2")));
  }
}
