/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.server.manager.state;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.server.init.ZooKeeperInitializer.getInitialRootTabletJson;
import static org.easymock.EasyMock.expect;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.SortedMap;

import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TabletHostingGoal;
import org.apache.accumulo.core.data.ConditionalMutation;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.manager.state.ManagerTabletInfo;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.SuspendingTServer;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.TabletFile;
import org.apache.accumulo.core.metadata.TabletState;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionMetadata;
import org.apache.accumulo.core.metadata.schema.MetadataTime;
import org.apache.accumulo.core.metadata.schema.RootTabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.LocationType;
import org.apache.accumulo.core.metadata.schema.TabletOperationId;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.server.MockServerContext;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.metadata.ConditionalTabletsMutatorImpl;
import org.apache.hadoop.io.Text;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Test;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.net.HostAndPort;

public class RootTabletStateStoreTest {

  private static class BrokenTabletMetadata extends TabletMetadata {
    private final TabletMetadata tm;

    BrokenTabletMetadata(TabletMetadata tm) {
      this.tm = tm;
    }

    public boolean equals(Object obj) {
      return tm.equals(obj);
    }

    public TableId getTableId() {
      return TableId.of("0");
    }

    public KeyExtent getExtent() {
      return new KeyExtent(TableId.of("0"), null, null);
    }

    public Text getPrevEndRow() {
      return tm.getPrevEndRow();
    }

    public boolean sawPrevEndRow() {
      return tm.sawPrevEndRow();
    }

    public Text getOldPrevEndRow() {
      return tm.getOldPrevEndRow();
    }

    public boolean sawOldPrevEndRow() {
      return tm.sawOldPrevEndRow();
    }

    public Text getEndRow() {
      return tm.getEndRow();
    }

    public Location getLocation() {
      return tm.getLocation();
    }

    public boolean hasCurrent() {
      return tm.hasCurrent();
    }

    public Map<TabletFile,Long> getLoaded() {
      return tm.getLoaded();
    }

    public Location getLast() {
      return tm.getLast();
    }

    public SuspendingTServer getSuspend() {
      return tm.getSuspend();
    }

    public Collection<StoredTabletFile> getFiles() {
      return tm.getFiles();
    }

    public Map<StoredTabletFile,DataFileValue> getFilesMap() {
      return tm.getFilesMap();
    }

    public Collection<LogEntry> getLogs() {
      return tm.getLogs();
    }

    public List<StoredTabletFile> getScans() {
      return tm.getScans();
    }

    public String getDirName() {
      return tm.getDirName();
    }

    public MetadataTime getTime() {
      return tm.getTime();
    }

    public String getCloned() {
      return tm.getCloned();
    }

    public OptionalLong getFlushId() {
      return tm.getFlushId();
    }

    public OptionalLong getCompactId() {
      return tm.getCompactId();
    }

    public Double getSplitRatio() {
      return tm.getSplitRatio();
    }

    public boolean hasChopped() {
      return tm.hasChopped();
    }

    public TabletHostingGoal getHostingGoal() {
      return tm.getHostingGoal();
    }

    public boolean getHostingRequested() {
      return tm.getHostingRequested();
    }

    public SortedMap<Key,Value> getKeyValues() {
      return tm.getKeyValues();
    }

    public TabletState getTabletState(Set<TServerInstance> liveTServers) {
      return tm.getTabletState(liveTServers);
    }

    public Map<ExternalCompactionId,ExternalCompactionMetadata> getExternalCompactions() {
      return tm.getExternalCompactions();
    }

    public TabletOperationId getOperationId() {
      return tm.getOperationId();
    }

    public int hashCode() {
      return tm.hashCode();
    }

    public String toString() {
      return tm.toString();
    }
  }

  private static class TestAmple implements Ample {

    private String json =
        new String(getInitialRootTabletJson("dir", "hdfs://nn/acc/tables/some/dir/0000.rf"), UTF_8);

    @Override
    public TabletMetadata readTablet(KeyExtent extent, ReadConsistency rc,
        ColumnType... colsToFetch) {
      Preconditions.checkArgument(extent.equals(RootTable.EXTENT));
      return new RootTabletMetadata(json).toTabletMetadata();
    }

    @Override
    public TabletsMetadata.TableOptions readTablets() {
      throw new UnsupportedOperationException("This method should be implemented in subclasses");
    }

    public ConditionalTabletsMutator conditionallyMutateTablets() {
      return new ConditionalTabletsMutatorImpl(null) {
        protected ConditionalWriter createConditionalWriter(Ample.DataLevel dataLevel)
            throws TableNotFoundException {
          Preconditions.checkArgument(dataLevel == DataLevel.ROOT);
          return new ConditionalWriter() {
            @Override
            public Iterator<Result> write(Iterator<ConditionalMutation> mutations) {
              return Iterators.transform(mutations, this::write);
            }

            @Override
            public Result write(ConditionalMutation mutation) {
              var rtm = new RootTabletMetadata(json);
              rtm.update(mutation);
              json = rtm.toJson();
              return new Result(Status.ACCEPTED, mutation, "server");
            }

            @Override
            public void close() {

            }
          };
        }
      };
    }

  }

  @Test
  public void testRootTabletStateStore() throws DistributedStoreException {
    ServerContext context = MockServerContext.get();
    expect(context.getAmple()).andReturn(new TestAmple()).anyTimes();
    EasyMock.replay(context);
    ZooTabletStateStore tstore = new ZooTabletStateStore(context);
    KeyExtent root = RootTable.EXTENT;
    String sessionId = "this is my unique session data";
    TServerInstance server =
        new TServerInstance(HostAndPort.fromParts("127.0.0.1", 10000), sessionId);
    List<Assignment> assignments = Collections.singletonList(new Assignment(root, server, null));
    tstore.setFutureLocations(assignments);
    int count = 0;
    for (ManagerTabletInfo mti : tstore) {
      assertEquals(mti.getTabletMetadata().getExtent(), root);
      assertTrue(mti.getTabletMetadata().getLocation().getType().equals(LocationType.FUTURE));
      assertEquals(mti.getTabletMetadata().getLocation().getServerInstance(), server);
      assertFalse(mti.getTabletMetadata().hasCurrent());
      count++;
    }
    assertEquals(count, 1);
    tstore.setLocations(assignments);
    count = 0;
    for (ManagerTabletInfo mti : tstore) {
      assertEquals(mti.getTabletMetadata().getExtent(), root);
      assertFalse(mti.getTabletMetadata().getLocation().getType().equals(LocationType.FUTURE));
      assertTrue(mti.getTabletMetadata().hasCurrent());
      assertEquals(mti.getTabletMetadata().getLocation().getServerInstance(), server);
      count++;
    }
    assertEquals(count, 1);
    ManagerTabletInfo rootTabletMetadataInfo = tstore.iterator().next();
    tstore.unassign(Collections.singletonList(rootTabletMetadataInfo.getTabletMetadata()), null);
    count = 0;
    for (ManagerTabletInfo mti : tstore) {
      assertEquals(mti.getTabletMetadata().getExtent(), root);
      assertFalse(mti.getTabletMetadata().hasCurrent());
      assertNull(mti.getTabletMetadata().getLocation());
      count++;
    }
    assertEquals(count, 1);

    KeyExtent notRoot = new KeyExtent(TableId.of("0"), null, null);
    final var assignmentList = List.of(new Assignment(notRoot, server, null));
    assertThrows(IllegalArgumentException.class, () -> tstore.setLocations(assignmentList));
    assertThrows(IllegalArgumentException.class, () -> tstore.setFutureLocations(assignmentList));
    final List<TabletMetadata> assignmentList1 =
        List.of(new BrokenTabletMetadata(rootTabletMetadataInfo.getTabletMetadata()));
    assertThrows(IllegalArgumentException.class, () -> tstore.unassign(assignmentList1, null));
  }
}
