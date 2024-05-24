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
package org.apache.accumulo.manager.tableOps.merge;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.AVAILABILITY;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.CLONED;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.COMPACTED;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.DIR;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.ECOMP;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.FILES;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.FLUSH_ID;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.FLUSH_NONCE;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.HOSTING_REQUESTED;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LAST;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LOADED;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LOCATION;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LOGS;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.MERGED;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.OPID;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.PREV_ROW;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.SCANS;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.SELECTED;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.SUSPEND;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.TIME;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.UNSPLITTABLE;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.USER_COMPACTION_REQUESTED;
import static org.apache.accumulo.manager.tableOps.split.UpdateTabletsTest.newSTF;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.accumulo.core.client.admin.TabletAvailability;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.FateInstanceType;
import org.apache.accumulo.core.gc.ReferenceFile;
import org.apache.accumulo.core.lock.ServiceLock;
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
import org.apache.accumulo.core.metadata.schema.TabletMetadataBuilder;
import org.apache.accumulo.core.metadata.schema.TabletOperationId;
import org.apache.accumulo.core.metadata.schema.TabletOperationType;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.metadata.schema.UnSplittableMetadata;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.core.util.time.SteadyTime;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.gc.AllVolumesDirectory;
import org.apache.accumulo.server.metadata.ConditionalTabletMutatorImpl;
import org.apache.accumulo.server.metadata.ConditionalTabletsMutatorImpl;
import org.apache.hadoop.io.Text;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Test;

public class MergeTabletsTest {

  private static final TableId tableId = TableId.of("789");
  private static final FateId fateId = FateId.from(FateInstanceType.USER, UUID.randomUUID());
  private static final TabletOperationId opid =
      TabletOperationId.from(TabletOperationType.MERGING, fateId);

  private static final KeyExtent ke1 = new KeyExtent(tableId, new Text("c"), null);
  private static final KeyExtent ke2 = new KeyExtent(tableId, new Text("m"), new Text("c"));
  private static final KeyExtent ke3 = new KeyExtent(tableId, null, new Text("m"));

  /**
   * This is a set of tablet metadata columns that the merge code is known to handle. The purpose of
   * the set is to detect when a new tablet metadata column was added without considering the
   * implications for merging tablets. For a column to be in this set it means an Accumulo developer
   * has determined that merge code can handle that column OR has opened an issue about handling it.
   */
  private static final Set<TabletMetadata.ColumnType> COLUMNS_HANDLED_BY_MERGE =
      EnumSet.of(TIME, LOGS, FILES, PREV_ROW, OPID, LOCATION, ECOMP, SELECTED, LOADED,
          USER_COMPACTION_REQUESTED, MERGED, LAST, SCANS, DIR, CLONED, FLUSH_ID, FLUSH_NONCE,
          SUSPEND, AVAILABILITY, HOSTING_REQUESTED, COMPACTED, UNSPLITTABLE);

  /**
   * The purpose of this test is to catch new tablet metadata columns that were added w/o
   * considering merging tablets.
   */
  @Test
  public void checkColumns() {
    for (TabletMetadata.ColumnType columnType : TabletMetadata.ColumnType.values()) {
      assertTrue(COLUMNS_HANDLED_BY_MERGE.contains(columnType),
          "The merge code does not know how to handle " + columnType);
    }
  }

  @Test
  public void testManyColumns() throws Exception {

    // This test exercises the merge code with as many of the tablet metadata columns as possible.
    // For the columns FLUSH_ID and FLUSH_NONCE the merge code leaves them as is and that is ok.
    // This test should break if the code starts reading them though.

    var file1 = newSTF(1);
    var file2 = newSTF(2);
    var file3 = newSTF(3);
    var file4 = newSTF(4);

    var dfv1 = new DataFileValue(1000, 100, 20);
    var dfv2 = new DataFileValue(500, 50, 20);
    var dfv3 = new DataFileValue(4000, 400);
    var dfv4 = new DataFileValue(2000, 200);

    var cid1 = ExternalCompactionId.generate(UUID.randomUUID());
    var cid2 = ExternalCompactionId.generate(UUID.randomUUID());
    var cid3 = ExternalCompactionId.generate(UUID.randomUUID());

    var tabletTime = MetadataTime.parse("L30");
    var flushID = OptionalLong.of(40);
    var availability = TabletAvailability.HOSTED;
    var lastLocation = TabletMetadata.Location.last("1.2.3.4:1234", "123456789");
    var suspendingTServer = SuspendingTServer.fromValue(new Value("1.2.3.4:5|56"));

    var tablet1 = TabletMetadata.builder(ke1).putOperation(opid).putDirName("td1")
        .putFile(file3, dfv3).putTime(MetadataTime.parse("L3"))
        .putTabletAvailability(TabletAvailability.HOSTED).build(LOCATION, LOGS, FILES, ECOMP,
            MERGED, COMPACTED, SELECTED, USER_COMPACTION_REQUESTED, LOADED, CLONED);
    var tablet2 = TabletMetadata.builder(ke2).putOperation(opid).putDirName("td2")
        .putFile(file4, dfv4).putTime(MetadataTime.parse("L2"))
        .putTabletAvailability(TabletAvailability.HOSTED).build(LOCATION, LOGS, FILES, ECOMP,
            MERGED, COMPACTED, SELECTED, USER_COMPACTION_REQUESTED, LOADED, CLONED);

    var tabletFiles = Map.of(file1, dfv1, file2, dfv2);

    var unsplittableMeta =
        UnSplittableMetadata.toUnSplittable(ke3, 1000, 1001, 10, tabletFiles.keySet());

    // Setup the metadata for the last tablet in the merge range, this is that tablet that merge
    // will modify.
    TabletMetadata lastTabletMeta = EasyMock.mock(TabletMetadata.class);
    EasyMock.expect(lastTabletMeta.getTableId()).andReturn(ke3.tableId()).anyTimes();
    EasyMock.expect(lastTabletMeta.getExtent()).andReturn(ke3).atLeastOnce();
    EasyMock.expect(lastTabletMeta.getEndRow()).andReturn(ke3.endRow()).anyTimes();
    EasyMock.expect(lastTabletMeta.getOperationId()).andReturn(opid).atLeastOnce();
    EasyMock.expect(lastTabletMeta.getLocation()).andReturn(null).atLeastOnce();
    EasyMock.expect(lastTabletMeta.getLogs()).andReturn(List.of()).atLeastOnce();
    EasyMock.expect(lastTabletMeta.hasMerged()).andReturn(false).atLeastOnce();
    EasyMock.expect(lastTabletMeta.getCloned()).andReturn(null).atLeastOnce();
    Map<ExternalCompactionId,CompactionMetadata> compactions = EasyMock.mock(Map.class);
    EasyMock.expect(compactions.keySet()).andReturn(Set.of(cid1, cid2, cid3)).anyTimes();
    EasyMock.expect(lastTabletMeta.getExternalCompactions()).andReturn(compactions).atLeastOnce();
    EasyMock.expect(lastTabletMeta.getFilesMap()).andReturn(tabletFiles).atLeastOnce();
    EasyMock.expect(lastTabletMeta.getFiles()).andReturn(tabletFiles.keySet()).anyTimes();
    EasyMock.expect(lastTabletMeta.getSelectedFiles()).andReturn(null).atLeastOnce();
    EasyMock.expect(lastTabletMeta.getUserCompactionsRequested()).andReturn(Set.of()).atLeastOnce();
    EasyMock.expect(lastTabletMeta.getCompacted()).andReturn(Set.of()).atLeastOnce();
    EasyMock.expect(lastTabletMeta.getScans()).andReturn(List.of(file1, file2)).atLeastOnce();
    EasyMock.expect(lastTabletMeta.getTime()).andReturn(tabletTime).atLeastOnce();
    EasyMock.expect(lastTabletMeta.getFlushId()).andReturn(flushID).anyTimes();
    EasyMock.expect(lastTabletMeta.getTabletAvailability()).andReturn(availability).atLeastOnce();
    EasyMock.expect(lastTabletMeta.getLoaded()).andReturn(Map.of()).atLeastOnce();
    EasyMock.expect(lastTabletMeta.getHostingRequested()).andReturn(true).atLeastOnce();
    EasyMock.expect(lastTabletMeta.getSuspend()).andReturn(suspendingTServer).atLeastOnce();
    EasyMock.expect(lastTabletMeta.getLast()).andReturn(lastLocation).atLeastOnce();
    EasyMock.expect(lastTabletMeta.getUnSplittable()).andReturn(unsplittableMeta).atLeastOnce();

    EasyMock.replay(lastTabletMeta, compactions);

    testMerge(List.of(tablet1, tablet2, lastTabletMeta), tableId, null, null, tabletMutator -> {
      EasyMock.expect(tabletMutator.putTime(MetadataTime.parse("L30"))).andReturn(tabletMutator)
          .once();
      EasyMock.expect(tabletMutator.putTabletAvailability(TabletAvailability.HOSTED))
          .andReturn(tabletMutator).once();
      EasyMock.expect(tabletMutator.putPrevEndRow(ke1.prevEndRow())).andReturn(tabletMutator)
          .once();
      EasyMock.expect(tabletMutator.setMerged()).andReturn(tabletMutator).once();

      // set file update expectations
      EasyMock.expect(tabletMutator.deleteFile(file1)).andReturn(tabletMutator).once();
      EasyMock.expect(tabletMutator.deleteFile(file2)).andReturn(tabletMutator).once();
      var fencedFile1 = StoredTabletFile.of(file1.getPath(), ke3.toDataRange());
      var fencedFile2 = StoredTabletFile.of(file2.getPath(), ke3.toDataRange());
      var fencedFile3 = StoredTabletFile.of(file3.getPath(), ke1.toDataRange());
      var fencedFile4 = StoredTabletFile.of(file4.getPath(), ke2.toDataRange());
      EasyMock.expect(tabletMutator.putFile(fencedFile1, dfv1)).andReturn(tabletMutator).once();
      EasyMock.expect(tabletMutator.putFile(fencedFile2, dfv2)).andReturn(tabletMutator).once();
      EasyMock.expect(tabletMutator.putFile(fencedFile3, dfv3)).andReturn(tabletMutator).once();
      EasyMock.expect(tabletMutator.putFile(fencedFile4, dfv4)).andReturn(tabletMutator).once();

      EasyMock.expect(tabletMutator.deleteExternalCompaction(cid1)).andReturn(tabletMutator).once();
      EasyMock.expect(tabletMutator.deleteExternalCompaction(cid2)).andReturn(tabletMutator).once();
      EasyMock.expect(tabletMutator.deleteExternalCompaction(cid3)).andReturn(tabletMutator).once();

      EasyMock.expect(tabletMutator.deleteScan(file1)).andReturn(tabletMutator);
      EasyMock.expect(tabletMutator.deleteScan(file2)).andReturn(tabletMutator);

      EasyMock.expect(tabletMutator.deleteHostingRequested()).andReturn(tabletMutator);
      EasyMock.expect(tabletMutator.deleteSuspension()).andReturn(tabletMutator);
      EasyMock.expect(tabletMutator.deleteLocation(lastLocation)).andReturn(tabletMutator);
      EasyMock.expect(tabletMutator.deleteUnSplittable()).andReturn(tabletMutator);

    });

    EasyMock.verify(lastTabletMeta, compactions);
  }

  @Test
  public void testTwoLastTablets() {
    var tablet1 = TabletMetadata.builder(ke1).putOperation(opid).putDirName("td1")
        .putTime(MetadataTime.parse("L40")).putTabletAvailability(TabletAvailability.HOSTED)
        .build(LOCATION, LOGS, FILES, ECOMP, MERGED, COMPACTED, SELECTED, USER_COMPACTION_REQUESTED,
            LOADED, CLONED);
    var tablet2 = TabletMetadata.builder(ke2).putOperation(opid).putDirName("td2")
        .putTime(MetadataTime.parse("L30")).putTabletAvailability(TabletAvailability.HOSTED)
        .build(LOCATION, LOGS, FILES, ECOMP, MERGED, COMPACTED, SELECTED, USER_COMPACTION_REQUESTED,
            LOADED, CLONED);
    var tablet3 = TabletMetadata.builder(ke3).putOperation(opid).putDirName("td3")
        .putTime(MetadataTime.parse("L50")).putTabletAvailability(TabletAvailability.HOSTED)
        .build(LOCATION, LOGS, FILES, ECOMP, MERGED, COMPACTED, SELECTED, USER_COMPACTION_REQUESTED,
            LOADED, CLONED);

    // repeat tablet3 in the list of tablets to simulate two last tablets
    var exception = assertThrows(IllegalStateException.class,
        () -> testMerge(List.of(tablet1, tablet2, tablet3, tablet3), tableId, null, null,
            tabletMutator -> {}));
    assertTrue(exception.getMessage().contains(" unexpectedly saw multiple last tablets"));

  }

  @Test
  public void testMetadataHole() throws Exception {
    var tablet1 = TabletMetadata.builder(ke1).putOperation(opid).putDirName("td1")
        .putTime(MetadataTime.parse("L40")).putTabletAvailability(TabletAvailability.HOSTED)
        .build(LOCATION, LOGS, FILES, ECOMP, MERGED, COMPACTED, SELECTED, USER_COMPACTION_REQUESTED,
            LOADED, CLONED);
    var tablet3 = TabletMetadata.builder(ke3).putOperation(opid).putDirName("td1")
        .putTime(MetadataTime.parse("L50")).putTabletAvailability(TabletAvailability.HOSTED)
        .build(LOCATION, LOGS, FILES, ECOMP, MERGED, COMPACTED, SELECTED, USER_COMPACTION_REQUESTED,
            LOADED, CLONED);

    var exception = assertThrows(IllegalStateException.class,
        () -> testMerge(List.of(tablet1, tablet3), tableId, null, null, tabletMutator -> {}));
    assertTrue(exception.getMessage().contains(" unexpectedly saw a hole in the metadata table"));
  }

  @Test
  public void testMisplacedMerge() throws Exception {
    // the merge marker should occur in the last tablet, try putting it in the first tablet
    var tablet1 = TabletMetadata.builder(ke1).putOperation(opid).putDirName("td1")
        .putTime(MetadataTime.parse("L40")).putTabletAvailability(TabletAvailability.HOSTED)
        .setMerged().build(LOCATION, LOGS, FILES, ECOMP, MERGED, COMPACTED, SELECTED,
            USER_COMPACTION_REQUESTED, LOADED, CLONED);
    var tablet2 = TabletMetadata.builder(ke2).putOperation(opid).putDirName("td1")
        .putTime(MetadataTime.parse("L50")).putTabletAvailability(TabletAvailability.HOSTED)
        .build(LOCATION, LOGS, FILES, ECOMP, MERGED, COMPACTED, SELECTED, USER_COMPACTION_REQUESTED,
            LOADED, CLONED);

    var exception = assertThrows(IllegalStateException.class,
        () -> testMerge(List.of(tablet1, tablet2), tableId, null, null, tabletMutator -> {}));
    assertTrue(exception.getMessage().contains(" has unexpected merge marker"));
  }

  @Test
  public void testUnexpectedColumns() {
    var tserver = new TServerInstance("1.2.3.4:1234", 123456789L);
    var futureLoc = TabletMetadata.Location.future(tserver);
    testUnexpectedColumn(tmb -> tmb.putLocation(futureLoc), "had location", futureLoc.toString());

    var currLoc = TabletMetadata.Location.current(tserver);
    testUnexpectedColumn(tmb -> tmb.putLocation(currLoc), "had location", currLoc.toString());

    var otherFateId = FateId.from(FateInstanceType.USER, UUID.randomUUID());
    var otherOpid = TabletOperationId.from(TabletOperationType.MERGING, otherFateId);
    testUnexpectedColumn(tmb -> tmb.putOperation(otherOpid), "had unexpected opid",
        otherOpid.toString());

    var walog = LogEntry.fromPath("localhost+8020/" + UUID.randomUUID());
    testUnexpectedColumn(tmb -> tmb.putWal(walog), "has unexpected walogs 1");

    FateId ucfid1 = otherFateId;
    testUnexpectedColumn(tmb -> tmb.putCompacted(ucfid1), "has unexpected compacted columns");

    testUnexpectedColumn(tmb -> tmb.putUserCompactionRequested(ucfid1),
        "has unexpected use compaction requested column");

    var lodedFile = newSTF(99);
    testUnexpectedColumn(tmb -> tmb.putBulkFile(lodedFile.getTabletFile(), otherFateId),
        "has unexpected loaded column");

    var selectedFiles = new SelectedFiles(Set.of(newSTF(567)), false, otherFateId,
        SteadyTime.from(100, TimeUnit.NANOSECONDS));
    testUnexpectedColumn(tmb -> tmb.putSelectedFiles(selectedFiles),
        "has unexpected selected file");

    // ELASTICITY_TODO need to test cloned marker, need to add it to TabletMetadataBuilder

  }

  private void testUnexpectedColumn(Consumer<TabletMetadataBuilder> badColumnSetter,
      String... expectedMessages) {
    assertTrue(expectedMessages.length >= 1);

    // run the test setting the bad column on the first, middle and last tablet

    for (int i = 0; i < 3; i++) {
      var builder1 = TabletMetadata.builder(ke1).putOperation(opid).putDirName("td1")
          .putTime(MetadataTime.parse("L40")).putTabletAvailability(TabletAvailability.HOSTED);
      if (i == 0) {
        badColumnSetter.accept(builder1);
      }
      var tablet1 = builder1.build(LOCATION, LOGS, FILES, ECOMP, MERGED, COMPACTED, SELECTED,
          USER_COMPACTION_REQUESTED, LOADED, CLONED);
      var builder2 = TabletMetadata.builder(ke2).putOperation(opid).putDirName("td1")
          .putTime(MetadataTime.parse("L70")).putTabletAvailability(TabletAvailability.HOSTED);
      if (i == 1) {
        badColumnSetter.accept(builder2);
      }
      var tablet2 = builder2.build(LOCATION, LOGS, FILES, ECOMP, MERGED, COMPACTED, SELECTED,
          USER_COMPACTION_REQUESTED, LOADED, CLONED);
      var builder3 = TabletMetadata.builder(ke3).putOperation(opid).putDirName("td1")
          .putTime(MetadataTime.parse("L50")).putTabletAvailability(TabletAvailability.HOSTED);
      if (i == 2) {
        badColumnSetter.accept(builder3);
      }
      var tablet3 = builder3.build(LOCATION, LOGS, FILES, ECOMP, MERGED, COMPACTED, SELECTED,
          USER_COMPACTION_REQUESTED, LOADED, CLONED);

      for (var expectedMessage : expectedMessages) {
        var exception = assertThrows(IllegalStateException.class,
            () -> testMerge(List.of(tablet1, tablet2, tablet3), tableId, null, null,
                tabletMutator -> {}));
        assertTrue(exception.getMessage().contains(expectedMessage),
            () -> exception.getMessage() + " did not contain " + expectedMessage);
      }
    }
  }

  @Test
  public void testTime() throws Exception {

    // this test ensures the max time is taken from all tablets
    String testTimes[][] = {{"L30", "L20", "L10"}, {"L20", "L30", "L10"}, {"L10", "L20", "L30"}};

    for (String[] times : testTimes) {

      var tablet1 = TabletMetadata.builder(ke1).putOperation(opid).putDirName("td1")
          .putTime(MetadataTime.parse(times[0])).putTabletAvailability(TabletAvailability.HOSTED)
          .build(LOCATION, LOGS, FILES, ECOMP, MERGED, COMPACTED, SELECTED,
              USER_COMPACTION_REQUESTED, LOADED, CLONED, SCANS, HOSTING_REQUESTED, SUSPEND, LAST,
              UNSPLITTABLE);
      var tablet2 = TabletMetadata.builder(ke2).putOperation(opid).putDirName("td2")
          .putTime(MetadataTime.parse(times[1])).putTabletAvailability(TabletAvailability.HOSTED)
          .build(LOCATION, LOGS, FILES, ECOMP, MERGED, COMPACTED, SELECTED,
              USER_COMPACTION_REQUESTED, LOADED, CLONED, SCANS, HOSTING_REQUESTED, SUSPEND, LAST,
              UNSPLITTABLE);
      var tablet3 = TabletMetadata.builder(ke3).putOperation(opid).putDirName("td3")
          .putTime(MetadataTime.parse(times[2])).putTabletAvailability(TabletAvailability.HOSTED)
          .build(LOCATION, LOGS, FILES, ECOMP, MERGED, COMPACTED, SELECTED,
              USER_COMPACTION_REQUESTED, LOADED, CLONED, SCANS, HOSTING_REQUESTED, SUSPEND, LAST,
              UNSPLITTABLE);

      testMerge(List.of(tablet1, tablet2, tablet3), tableId, null, null, tabletMutator -> {
        EasyMock.expect(tabletMutator.putTime(MetadataTime.parse("L30"))).andReturn(tabletMutator)
            .once();
        EasyMock.expect(tabletMutator.putTabletAvailability(TabletAvailability.HOSTED))
            .andReturn(tabletMutator).once();
        EasyMock.expect(tabletMutator.putPrevEndRow(ke1.prevEndRow())).andReturn(tabletMutator)
            .once();
        EasyMock.expect(tabletMutator.setMerged()).andReturn(tabletMutator).once();
      });
    }

  }

  private static void testMerge(List<TabletMetadata> inputTablets, TableId tableId, String start,
      String end, Consumer<ConditionalTabletMutatorImpl> expectationsSetter) throws Exception {
    MergeInfo mergeInfo =
        new MergeInfo(tableId, NamespaceId.of("1"), start == null ? null : start.getBytes(UTF_8),
            end == null ? null : end.getBytes(UTF_8), MergeInfo.Operation.MERGE);
    MergeTablets mergeTablets = new MergeTablets(mergeInfo);

    Manager manager = EasyMock.mock(Manager.class);
    ServerContext context = EasyMock.mock(ServerContext.class);
    Ample ample = EasyMock.mock(Ample.class);
    TabletsMetadata.Builder tabletBuilder = EasyMock.mock(TabletsMetadata.Builder.class);
    TabletsMetadata tabletsMetadata = EasyMock.mock(TabletsMetadata.class);
    ConditionalTabletsMutatorImpl tabletsMutator =
        EasyMock.mock(ConditionalTabletsMutatorImpl.class);
    ConditionalTabletMutatorImpl tabletMutator = EasyMock.mock(ConditionalTabletMutatorImpl.class);

    ServiceLock managerLock = EasyMock.mock(ServiceLock.class);
    EasyMock.expect(context.getServiceLock()).andReturn(managerLock).anyTimes();

    // setup reading the tablets
    EasyMock.expect(manager.getContext()).andReturn(context).atLeastOnce();
    EasyMock.expect(context.getAmple()).andReturn(ample).atLeastOnce();
    EasyMock.expect(ample.readTablets()).andReturn(tabletBuilder).once();
    EasyMock.expect(tabletBuilder.forTable(tableId)).andReturn(tabletBuilder).once();
    EasyMock.expect(tabletBuilder.overlapping(mergeInfo.getMergeExtent().prevEndRow(),
        mergeInfo.getMergeExtent().endRow())).andReturn(tabletBuilder).once();
    EasyMock.expect(tabletBuilder.build()).andReturn(tabletsMetadata).once();
    EasyMock.expect(tabletsMetadata.iterator()).andReturn(inputTablets.iterator()).once();
    tabletsMetadata.close();
    EasyMock.expectLastCall().once();

    var lastExtent = inputTablets.get(inputTablets.size() - 1).getExtent();

    // setup writing the tablets
    EasyMock.expect(ample.conditionallyMutateTablets()).andReturn(tabletsMutator).once();
    EasyMock.expect(tabletsMutator.mutateTablet(lastExtent)).andReturn(tabletMutator).once();
    EasyMock.expect(tabletMutator.requireOperation(opid)).andReturn(tabletMutator).once();
    EasyMock.expect(tabletMutator.requireAbsentLocation()).andReturn(tabletMutator).once();
    EasyMock.expect(tabletMutator.requireAbsentLogs()).andReturn(tabletMutator).once();

    expectationsSetter.accept(tabletMutator);

    tabletMutator.submit(EasyMock.anyObject());
    EasyMock.expectLastCall().once();

    // setup processing of conditional mutations
    Ample.ConditionalResult cr = EasyMock.niceMock(Ample.ConditionalResult.class);
    EasyMock.expect(cr.getStatus()).andReturn(Ample.ConditionalResult.Status.ACCEPTED)
        .atLeastOnce();
    EasyMock.expect(tabletsMutator.process()).andReturn(Map.of(lastExtent, cr)).atLeastOnce();
    tabletsMutator.close();
    EasyMock.expectLastCall().anyTimes();

    // Setup GC candidate write
    List<ReferenceFile> dirs = new ArrayList<>();
    dirs.add(new AllVolumesDirectory(tableId, "td1"));
    dirs.add(new AllVolumesDirectory(tableId, "td2"));
    ample.putGcFileAndDirCandidates(tableId, dirs);
    EasyMock.expectLastCall().once();

    EasyMock.replay(manager, context, ample, tabletBuilder, tabletsMetadata, tabletsMutator,
        tabletMutator, cr, managerLock);

    mergeTablets.call(fateId, manager);

    EasyMock.verify(manager, context, ample, tabletBuilder, tabletsMetadata, tabletsMutator,
        tabletMutator, cr, managerLock);
  }
}
