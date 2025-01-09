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
package org.apache.accumulo.manager.tableOps.split;

import static org.easymock.EasyMock.mock;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;

import org.apache.accumulo.core.client.admin.TabletAvailability;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.FateInstanceType;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.metadata.ReferencedTabletFile;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.SuspendingTServer;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.CompactionMetadata;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.metadata.schema.MetadataTime;
import org.apache.accumulo.core.metadata.schema.SelectedFiles;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletOperationId;
import org.apache.accumulo.core.metadata.schema.TabletOperationType;
import org.apache.accumulo.core.metadata.schema.UnSplittableMetadata;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.split.Splitter;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.metadata.ConditionalTabletMutatorImpl;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Test;

public class UpdateTabletsTest {

  public static StoredTabletFile newSTF(int fileNum) {
    return new ReferencedTabletFile(new Path(
        "hdfs://localhost:8020/accumulo/tables/2a/default_tablet/F00000" + fileNum + ".rf"))
        .insert();
  }

  Splitter.FileInfo newFileInfo(String start, String end) {
    return new Splitter.FileInfo(new Text(start), new Text(end));
  }

  /**
   * This is a set of tablet metadata columns that the split code is known to handle. The purpose of
   * the set is to detect when a new tablet metadata column was added without considering the
   * implications for splitting tablets. For a column to be in this set it means an Accumulo
   * developer has determined that split code can handle that column OR has opened an issue about
   * handling it.
   */
  private static final Set<ColumnType> COLUMNS_HANDLED_BY_SPLIT =
      EnumSet.of(ColumnType.TIME, ColumnType.LOGS, ColumnType.FILES, ColumnType.PREV_ROW,
          ColumnType.OPID, ColumnType.LOCATION, ColumnType.ECOMP, ColumnType.SELECTED,
          ColumnType.LOADED, ColumnType.USER_COMPACTION_REQUESTED, ColumnType.MERGED,
          ColumnType.LAST, ColumnType.SCANS, ColumnType.DIR, ColumnType.CLONED, ColumnType.FLUSH_ID,
          ColumnType.FLUSH_NONCE, ColumnType.SUSPEND, ColumnType.AVAILABILITY,
          ColumnType.HOSTING_REQUESTED, ColumnType.COMPACTED, ColumnType.UNSPLITTABLE);

  /**
   * The purpose of this test is to catch new tablet metadata columns that were added w/o
   * considering splitting tablets.
   */
  @Test
  public void checkColumns() {
    for (ColumnType columnType : ColumnType.values()) {
      assertTrue(COLUMNS_HANDLED_BY_SPLIT.contains(columnType),
          "The split code does not know how to handle " + columnType);
    }
  }

  // When a tablet splits its files are partitioned among the new children tablets. This test
  // exercises the partitioning code.
  @Test
  public void testFileParitioning() {

    var file1 = newSTF(1);
    var file2 = newSTF(2);
    var file3 = newSTF(3);
    var file4 = newSTF(4);

    var tabletFiles =
        Map.of(file1, new DataFileValue(1000, 100, 20), file2, new DataFileValue(2000, 200, 50),
            file3, new DataFileValue(4000, 400), file4, new DataFileValue(4000, 400));

    var ke1 = new KeyExtent(TableId.of("1"), new Text("m"), null);
    var ke2 = new KeyExtent(TableId.of("1"), new Text("r"), new Text("m"));
    var ke3 = new KeyExtent(TableId.of("1"), new Text("v"), new Text("r"));
    var ke4 = new KeyExtent(TableId.of("1"), null, new Text("v"));

    var firstAndLastKeys = Map.of(file2, newFileInfo("m", "r"), file3, newFileInfo("g", "x"), file4,
        newFileInfo("s", "v"));

    var ke1Expected = Map.of(file1, new DataFileValue(250, 25, 20), file2,
        new DataFileValue(1000, 100, 50), file3, new DataFileValue(1000, 100));
    var ke2Expected = Map.of(file1, new DataFileValue(250, 25, 20), file2,
        new DataFileValue(1000, 100, 50), file3, new DataFileValue(1000, 100));
    var ke3Expected = Map.of(file1, new DataFileValue(250, 25, 20), file3,
        new DataFileValue(1000, 100), file4, new DataFileValue(4000, 400));
    var ke4Expected =
        Map.of(file1, new DataFileValue(250, 25, 20), file3, new DataFileValue(1000, 100));

    var expected = Map.of(ke1, ke1Expected, ke2, ke2Expected, ke3, ke3Expected, ke4, ke4Expected);

    Set<KeyExtent> newExtents = Set.of(ke1, ke2, ke3, ke4);

    TabletMetadata tabletMeta = EasyMock.createMock(TabletMetadata.class);
    EasyMock.expect(tabletMeta.getFilesMap()).andReturn(tabletFiles).anyTimes();
    EasyMock.replay(tabletMeta);

    Map<KeyExtent,Map<StoredTabletFile,DataFileValue>> results =
        UpdateTablets.getNewTabletFiles(newExtents, tabletMeta, firstAndLastKeys::get);

    assertEquals(expected.keySet(), results.keySet());
    expected.forEach(((extent, files) -> {
      assertEquals(files, results.get(extent));
    }));

    // Test a tablet with no files going to it

    var tabletFiles2 = Map.of(file2, tabletFiles.get(file2), file4, tabletFiles.get(file4));
    ke1Expected = Map.of(file2, new DataFileValue(1000, 100, 50));
    ke2Expected = Map.of(file2, new DataFileValue(1000, 100, 50));
    ke3Expected = Map.of(file4, new DataFileValue(4000, 400));
    ke4Expected = Map.of();
    expected = Map.of(ke1, ke1Expected, ke2, ke2Expected, ke3, ke3Expected, ke4, ke4Expected);

    tabletMeta = EasyMock.createMock(TabletMetadata.class);
    EasyMock.expect(tabletMeta.getFilesMap()).andReturn(tabletFiles2).anyTimes();
    EasyMock.replay(tabletMeta);

    Map<KeyExtent,Map<StoredTabletFile,DataFileValue>> results2 =
        UpdateTablets.getNewTabletFiles(newExtents, tabletMeta, firstAndLastKeys::get);
    assertEquals(expected.keySet(), results2.keySet());
    expected.forEach(((extent, files) -> {
      assertEquals(files, results2.get(extent));
    }));

  }

  /**
   * The purpose of this test is create tablet with as many columns in its metadata set as possible
   * and exercise the split code with that tablet.
   */
  @Test
  public void testManyColumns() throws Exception {
    TableId tableId = TableId.of("123");
    KeyExtent origExtent = new KeyExtent(tableId, new Text("m"), null);

    var newExtent1 = new KeyExtent(tableId, new Text("c"), null);
    var newExtent2 = new KeyExtent(tableId, new Text("h"), new Text("c"));
    var newExtent3 = new KeyExtent(tableId, new Text("m"), new Text("h"));

    var file1 = newSTF(1);
    var file2 = newSTF(2);
    var file3 = newSTF(3);
    var file4 = newSTF(4);

    var loaded1 = newSTF(5);
    var loaded2 = newSTF(6);

    var flid1 = FateId.from(FateInstanceType.USER, UUID.randomUUID());
    var flid2 = FateId.from(FateInstanceType.USER, UUID.randomUUID());
    var loaded = Map.of(loaded1, flid1, loaded2, flid2);

    var dfv1 = new DataFileValue(1000, 100, 20);
    var dfv2 = new DataFileValue(500, 50, 20);
    var dfv3 = new DataFileValue(4000, 400);
    var dfv4 = new DataFileValue(2000, 200);

    var tabletFiles = Map.of(file1, dfv1, file2, dfv2, file3, dfv3, file4, dfv4);

    var cid1 = ExternalCompactionId.generate(UUID.randomUUID());
    var cid2 = ExternalCompactionId.generate(UUID.randomUUID());
    var cid3 = ExternalCompactionId.generate(UUID.randomUUID());

    var fateId = FateId.from(FateInstanceType.USER, UUID.randomUUID());
    var opid = TabletOperationId.from(TabletOperationType.SPLITTING, fateId);
    var tabletTime = MetadataTime.parse("L30");
    var flushID = OptionalLong.of(40);
    var availability = TabletAvailability.HOSTED;
    var lastLocation = TabletMetadata.Location.last("1.2.3.4:1234", "123456789");
    var suspendingTServer = SuspendingTServer.fromValue(new Value("1.2.3.4:5|56"));

    String dir1 = "dir1";
    String dir2 = "dir2";

    Manager manager = EasyMock.mock(Manager.class);
    ServerContext context = EasyMock.mock(ServerContext.class);
    EasyMock.expect(manager.getContext()).andReturn(context).atLeastOnce();
    Ample ample = EasyMock.mock(Ample.class);
    EasyMock.expect(context.getAmple()).andReturn(ample).atLeastOnce();
    Splitter splitter = EasyMock.mock(Splitter.class);
    EasyMock.expect(splitter.getCachedFileInfo(tableId, file1)).andReturn(newFileInfo("a", "z"));
    EasyMock.expect(splitter.getCachedFileInfo(tableId, file2)).andReturn(newFileInfo("a", "b"));
    EasyMock.expect(splitter.getCachedFileInfo(tableId, file3)).andReturn(newFileInfo("d", "f"));
    EasyMock.expect(splitter.getCachedFileInfo(tableId, file4)).andReturn(newFileInfo("d", "j"));
    EasyMock.expect(manager.getSplitter()).andReturn(splitter).atLeastOnce();

    ServiceLock managerLock = EasyMock.mock(ServiceLock.class);
    EasyMock.expect(context.getServiceLock()).andReturn(managerLock).anyTimes();

    // Setup the metadata for the tablet that is going to split, set as many columns as possible on
    // it.
    TabletMetadata tabletMeta = EasyMock.mock(TabletMetadata.class);
    EasyMock.expect(tabletMeta.getExtent()).andReturn(origExtent).atLeastOnce();
    EasyMock.expect(tabletMeta.getOperationId()).andReturn(opid).atLeastOnce();
    EasyMock.expect(tabletMeta.getLocation()).andReturn(null).atLeastOnce();
    EasyMock.expect(tabletMeta.getLogs()).andReturn(List.of()).atLeastOnce();
    EasyMock.expect(tabletMeta.hasMerged()).andReturn(false).atLeastOnce();
    EasyMock.expect(tabletMeta.getCloned()).andReturn(null).atLeastOnce();
    Map<ExternalCompactionId,CompactionMetadata> compactions = EasyMock.mock(Map.class);
    EasyMock.expect(compactions.keySet()).andReturn(Set.of(cid1, cid2, cid3)).anyTimes();
    EasyMock.expect(tabletMeta.getExternalCompactions()).andReturn(compactions).atLeastOnce();
    EasyMock.expect(tabletMeta.getFilesMap()).andReturn(tabletFiles).atLeastOnce();
    EasyMock.expect(tabletMeta.getFiles()).andReturn(tabletFiles.keySet());
    SelectedFiles selectedFiles = EasyMock.mock(SelectedFiles.class);
    EasyMock.expect(selectedFiles.getFateId()).andReturn(null);
    EasyMock.expect(tabletMeta.getSelectedFiles()).andReturn(selectedFiles).atLeastOnce();
    FateId ucfid1 = FateId.from(FateInstanceType.USER, UUID.randomUUID());
    FateId ucfid2 = FateId.from(FateInstanceType.USER, UUID.randomUUID());
    EasyMock.expect(tabletMeta.getUserCompactionsRequested()).andReturn(Set.of(ucfid1, ucfid2))
        .atLeastOnce();
    FateId ucfid3 = FateId.from(FateInstanceType.USER, UUID.randomUUID());
    EasyMock.expect(tabletMeta.getCompacted()).andReturn(Set.of(ucfid1, ucfid3)).atLeastOnce();
    EasyMock.expect(tabletMeta.getScans()).andReturn(List.of(file1, file2)).atLeastOnce();
    EasyMock.expect(tabletMeta.getTime()).andReturn(tabletTime).atLeastOnce();
    EasyMock.expect(tabletMeta.getFlushId()).andReturn(flushID).atLeastOnce();
    EasyMock.expect(tabletMeta.getTabletAvailability()).andReturn(availability).atLeastOnce();
    EasyMock.expect(tabletMeta.getLoaded()).andReturn(loaded).atLeastOnce();
    EasyMock.expect(tabletMeta.getHostingRequested()).andReturn(true).atLeastOnce();
    EasyMock.expect(tabletMeta.getSuspend()).andReturn(suspendingTServer).atLeastOnce();
    EasyMock.expect(tabletMeta.getLast()).andReturn(lastLocation).atLeastOnce();
    UnSplittableMetadata usm =
        UnSplittableMetadata.toUnSplittable(origExtent, 1000, 1001, 1002, tabletFiles.keySet());
    EasyMock.expect(tabletMeta.getUnSplittable()).andReturn(usm).atLeastOnce();

    EasyMock.expect(ample.readTablet(origExtent)).andReturn(tabletMeta);

    Ample.ConditionalTabletsMutator tabletsMutator =
        EasyMock.mock(Ample.ConditionalTabletsMutator.class);
    EasyMock.expect(ample.conditionallyMutateTablets()).andReturn(tabletsMutator).atLeastOnce();

    // Setup the mutator for creating the first new tablet
    ConditionalTabletMutatorImpl tablet1Mutator = EasyMock.mock(ConditionalTabletMutatorImpl.class);
    EasyMock.expect(tablet1Mutator.requireAbsentTablet()).andReturn(tablet1Mutator);
    EasyMock.expect(tablet1Mutator.putOperation(opid)).andReturn(tablet1Mutator);
    EasyMock.expect(tablet1Mutator.putDirName(dir1)).andReturn(tablet1Mutator);
    EasyMock.expect(tablet1Mutator.putTime(tabletTime)).andReturn(tablet1Mutator);
    EasyMock.expect(tablet1Mutator.putFlushId(flushID.getAsLong())).andReturn(tablet1Mutator);
    EasyMock.expect(tablet1Mutator.putPrevEndRow(newExtent1.prevEndRow()))
        .andReturn(tablet1Mutator);
    EasyMock.expect(tablet1Mutator.putCompacted(ucfid1)).andReturn(tablet1Mutator);
    EasyMock.expect(tablet1Mutator.putCompacted(ucfid3)).andReturn(tablet1Mutator);
    EasyMock.expect(tablet1Mutator.putTabletAvailability(availability)).andReturn(tablet1Mutator);
    EasyMock.expect(tablet1Mutator.putBulkFile(loaded1.getTabletFile(), flid1))
        .andReturn(tablet1Mutator);
    EasyMock.expect(tablet1Mutator.putBulkFile(loaded2.getTabletFile(), flid2))
        .andReturn(tablet1Mutator);
    EasyMock.expect(tablet1Mutator.putFile(file1, new DataFileValue(333, 33, 20)))
        .andReturn(tablet1Mutator);
    EasyMock.expect(tablet1Mutator.putFile(file2, dfv2)).andReturn(tablet1Mutator);
    tablet1Mutator.submit(EasyMock.anyObject());
    EasyMock.expectLastCall().once();
    EasyMock.expect(tabletsMutator.mutateTablet(newExtent1)).andReturn(tablet1Mutator);

    // Setup the mutator for creating the second new tablet
    ConditionalTabletMutatorImpl tablet2Mutator = EasyMock.mock(ConditionalTabletMutatorImpl.class);
    EasyMock.expect(tablet2Mutator.requireAbsentTablet()).andReturn(tablet2Mutator);
    EasyMock.expect(tablet2Mutator.putOperation(opid)).andReturn(tablet2Mutator);
    EasyMock.expect(tablet2Mutator.putDirName(dir2)).andReturn(tablet2Mutator);
    EasyMock.expect(tablet2Mutator.putTime(tabletTime)).andReturn(tablet2Mutator);
    EasyMock.expect(tablet2Mutator.putFlushId(flushID.getAsLong())).andReturn(tablet2Mutator);
    EasyMock.expect(tablet2Mutator.putPrevEndRow(newExtent2.prevEndRow()))
        .andReturn(tablet2Mutator);
    EasyMock.expect(tablet2Mutator.putCompacted(ucfid1)).andReturn(tablet2Mutator);
    EasyMock.expect(tablet2Mutator.putCompacted(ucfid3)).andReturn(tablet2Mutator);
    EasyMock.expect(tablet2Mutator.putTabletAvailability(availability)).andReturn(tablet2Mutator);
    EasyMock.expect(tablet2Mutator.putBulkFile(loaded1.getTabletFile(), flid1))
        .andReturn(tablet2Mutator);
    EasyMock.expect(tablet2Mutator.putBulkFile(loaded2.getTabletFile(), flid2))
        .andReturn(tablet2Mutator);
    EasyMock.expect(tablet2Mutator.putFile(file1, new DataFileValue(333, 33, 20)))
        .andReturn(tablet2Mutator);
    EasyMock.expect(tablet2Mutator.putFile(file3, dfv3)).andReturn(tablet2Mutator);
    EasyMock.expect(tablet2Mutator.putFile(file4, new DataFileValue(1000, 100)))
        .andReturn(tablet2Mutator);
    tablet2Mutator.submit(EasyMock.anyObject());
    EasyMock.expectLastCall().once();
    EasyMock.expect(tabletsMutator.mutateTablet(newExtent2)).andReturn(tablet2Mutator);

    // Setup the mutator for updating the existing tablet
    ConditionalTabletMutatorImpl tablet3Mutator = mock(ConditionalTabletMutatorImpl.class);
    EasyMock.expect(tablet3Mutator.requireOperation(opid)).andReturn(tablet3Mutator);
    EasyMock.expect(tablet3Mutator.requireAbsentLocation()).andReturn(tablet3Mutator);
    EasyMock.expect(tablet3Mutator.requireAbsentLogs()).andReturn(tablet3Mutator);
    EasyMock.expect(tablet3Mutator.putPrevEndRow(newExtent3.prevEndRow()))
        .andReturn(tablet3Mutator);
    EasyMock.expect(tablet3Mutator.putFile(file1, new DataFileValue(333, 33, 20)))
        .andReturn(tablet3Mutator);
    EasyMock.expect(tablet3Mutator.putFile(file4, new DataFileValue(1000, 100)))
        .andReturn(tablet3Mutator);
    EasyMock.expect(tablet3Mutator.deleteFile(file2)).andReturn(tablet3Mutator);
    EasyMock.expect(tablet3Mutator.deleteFile(file3)).andReturn(tablet3Mutator);
    EasyMock.expect(tablet3Mutator.deleteExternalCompaction(cid1)).andReturn(tablet3Mutator);
    EasyMock.expect(tablet3Mutator.deleteExternalCompaction(cid2)).andReturn(tablet3Mutator);
    EasyMock.expect(tablet3Mutator.deleteExternalCompaction(cid3)).andReturn(tablet3Mutator);
    EasyMock.expect(tablet3Mutator.deleteSelectedFiles()).andReturn(tablet3Mutator);
    EasyMock.expect(tablet3Mutator.deleteUserCompactionRequested(ucfid1)).andReturn(tablet3Mutator);
    EasyMock.expect(tablet3Mutator.deleteUserCompactionRequested(ucfid2)).andReturn(tablet3Mutator);
    EasyMock.expect(tablet3Mutator.deleteScan(file1)).andReturn(tablet3Mutator);
    EasyMock.expect(tablet3Mutator.deleteScan(file2)).andReturn(tablet3Mutator);
    EasyMock.expect(tablet3Mutator.deleteHostingRequested()).andReturn(tablet3Mutator);
    EasyMock.expect(tablet3Mutator.deleteSuspension()).andReturn(tablet3Mutator);
    EasyMock.expect(tablet3Mutator.deleteLocation(lastLocation)).andReturn(tablet3Mutator);
    EasyMock.expect(tablet3Mutator.deleteUnSplittable()).andReturn(tablet3Mutator);
    tablet3Mutator.submit(EasyMock.anyObject());
    EasyMock.expectLastCall().once();
    EasyMock.expect(tabletsMutator.mutateTablet(origExtent)).andReturn(tablet3Mutator);

    // setup processing of conditional mutations
    Ample.ConditionalResult cr = EasyMock.niceMock(Ample.ConditionalResult.class);
    EasyMock.expect(cr.getStatus()).andReturn(Ample.ConditionalResult.Status.ACCEPTED)
        .atLeastOnce();
    EasyMock.expect(tabletsMutator.process())
        .andReturn(Map.of(newExtent1, cr, newExtent2, cr, origExtent, cr)).atLeastOnce();
    tabletsMutator.close();
    EasyMock.expectLastCall().anyTimes();

    EasyMock.replay(manager, context, ample, tabletMeta, splitter, tabletsMutator, tablet1Mutator,
        tablet2Mutator, tablet3Mutator, cr, compactions);
    // Now we can actually test the split code that writes the new tablets with a bunch columns in
    // the original tablet
    SortedSet<Text> splits = new TreeSet<>(List.of(newExtent1.endRow(), newExtent2.endRow()));
    UpdateTablets updateTablets =
        new UpdateTablets(new SplitInfo(origExtent, splits), List.of(dir1, dir2));
    updateTablets.call(fateId, manager);

    EasyMock.verify(manager, context, ample, tabletMeta, splitter, tabletsMutator, tablet1Mutator,
        tablet2Mutator, tablet3Mutator, cr, compactions);
  }

  @Test
  public void testErrors() throws Exception {
    TableId tableId = TableId.of("123");
    KeyExtent origExtent = new KeyExtent(tableId, new Text("m"), null);

    var fateId = FateId.from(FateInstanceType.USER, UUID.randomUUID());
    var opid = TabletOperationId.from(TabletOperationType.SPLITTING, fateId);

    // Test splitting a tablet with a location
    var location = TabletMetadata.Location.future(new TServerInstance("1.2.3.4:1234", 123456789L));
    var tablet1 =
        TabletMetadata.builder(origExtent).putOperation(opid).putLocation(location).build();
    var e = assertThrows(IllegalStateException.class, () -> testError(origExtent, tablet1, fateId));
    assertTrue(e.getMessage().contains(location.toString()));

    // Test splitting a tablet without an operation id set
    var tablet2 = TabletMetadata.builder(origExtent).build(ColumnType.OPID);
    e = assertThrows(IllegalStateException.class, () -> testError(origExtent, tablet2, fateId));
    assertTrue(e.getMessage().contains("does not have expected operation id "));
    assertTrue(e.getMessage().contains("null"));

    // Test splitting a tablet with an unexpected operation id
    var fateId2 = FateId.from(FateInstanceType.USER, UUID.randomUUID());
    var opid2 = TabletOperationId.from(TabletOperationType.SPLITTING, fateId2);
    var tablet3 = TabletMetadata.builder(origExtent).putOperation(opid2).build();
    e = assertThrows(IllegalStateException.class, () -> testError(origExtent, tablet3, fateId));
    assertTrue(e.getMessage().contains("does not have expected operation id "));
    assertTrue(e.getMessage().contains(opid2.toString()));

    // Test splitting a tablet with walogs
    var walog = LogEntry.fromPath("localhost+8020/" + UUID.randomUUID());
    var tablet4 = TabletMetadata.builder(origExtent).putOperation(opid).putWal(walog)
        .build(ColumnType.LOCATION);
    e = assertThrows(IllegalStateException.class, () -> testError(origExtent, tablet4, fateId));
    assertTrue(e.getMessage().contains("unexpectedly had walogs"));
    assertTrue(e.getMessage().contains(walog.toString()));

    // Test splitting tablet with merged marker
    var tablet5 = TabletMetadata.builder(origExtent).putOperation(opid).setMerged()
        .build(ColumnType.LOCATION, ColumnType.LOGS);
    e = assertThrows(IllegalStateException.class, () -> testError(origExtent, tablet5, fateId));
    assertTrue(e.getMessage().contains("unexpectedly has a merged"));

    // Test splitting tablet with cloned marker
    TabletMetadata tablet6 = EasyMock.mock(TabletMetadata.class);
    EasyMock.expect(tablet6.getExtent()).andReturn(origExtent).anyTimes();
    EasyMock.expect(tablet6.getOperationId()).andReturn(opid).anyTimes();
    EasyMock.expect(tablet6.getLocation()).andReturn(null).anyTimes();
    EasyMock.expect(tablet6.getLogs()).andReturn(List.of()).anyTimes();
    EasyMock.expect(tablet6.hasMerged()).andReturn(false);
    EasyMock.expect(tablet6.getCloned()).andReturn("OK").atLeastOnce();
    EasyMock.replay(tablet6);
    e = assertThrows(IllegalStateException.class, () -> testError(origExtent, tablet6, fateId));
    assertTrue(e.getMessage().contains("unexpectedly has a cloned"));
    EasyMock.verify(tablet6);
  }

  private static void testError(KeyExtent origExtent, TabletMetadata tm1, FateId fateId)
      throws Exception {
    Manager manager = EasyMock.mock(Manager.class);
    ServerContext context = EasyMock.mock(ServerContext.class);
    EasyMock.expect(manager.getContext()).andReturn(context).atLeastOnce();
    Ample ample = EasyMock.mock(Ample.class);
    EasyMock.expect(context.getAmple()).andReturn(ample).atLeastOnce();

    EasyMock.expect(ample.readTablet(origExtent)).andReturn(tm1);

    EasyMock.replay(manager, context, ample);
    // Now we can actually test the split code that writes the new tablets with a bunch columns in
    // the original tablet
    SortedSet<Text> splits = new TreeSet<>(List.of(new Text("c")));
    UpdateTablets updateTablets =
        new UpdateTablets(new SplitInfo(origExtent, splits), List.of("d1"));
    updateTablets.call(fateId, manager);

    EasyMock.verify(manager, context, ample);
  }
}
