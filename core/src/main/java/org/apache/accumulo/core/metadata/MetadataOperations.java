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
package org.apache.accumulo.core.metadata;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.MetadataTime;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.hadoop.io.Text;

import com.google.common.base.Preconditions;

public class MetadataOperations {

  public static void compact(Ample.ConditionalTabletsMutator ctm, KeyExtent extent,
      Set<StoredTabletFile> inputFiles, TabletFile outputFile, DataFileValue dfv) {
    Ample.ConditionalTabletMutator conditionalMutator = ctm.mutateTablet(extent);

    conditionalMutator.requireOperation(TabletOperation.NONE);
    inputFiles.forEach(conditionalMutator::requireFile);
    // TODO ensure this does not require the encoded version
    conditionalMutator.requirePrevEndRow(extent.prevEndRow());
    // TODO could add a conditional check to ensure file is not already there

    inputFiles.forEach(conditionalMutator::deleteFile);
    conditionalMutator.putFile(outputFile, dfv);

    conditionalMutator.submit();
  }

  public static void minorCompact(Ample.ConditionalTabletsMutator ctm, KeyExtent extent,
      TServerInstance tsi, TabletFile newFile, DataFileValue dfv) {
    Ample.ConditionalTabletMutator conditionalMutator = ctm.mutateTablet(extent);

    // when a tablet has a current location its operation should always be none, so this check is a
    // bit redundant
    conditionalMutator.requireOperation(TabletOperation.NONE);
    conditionalMutator.requireLocation(tsi, TabletMetadata.LocationType.CURRENT);
    conditionalMutator.requirePrevEndRow(extent.prevEndRow());
    // TODO could add a conditional check to ensure file is not already there

    conditionalMutator.putFile(newFile, dfv);

    conditionalMutator.submit();
  }

  public static void bulkImport(Ample.ConditionalTabletsMutator ctm, KeyExtent extent,
      Map<TabletFile,DataFileValue> newFiles, long tid) {
    Ample.ConditionalTabletMutator conditionalMutator = ctm.mutateTablet(extent);

    conditionalMutator.requireOperation(TabletOperation.NONE);
    conditionalMutator.requirePrevEndRow(extent.prevEndRow());
    // TODO check bulk import status of file in tablet metadata... do not want to reimport a file
    // that was previously imported and compacted away

    newFiles.keySet().forEach(file -> conditionalMutator.putBulkFile(file, tid));
    newFiles.forEach(conditionalMutator::putFile);

    conditionalMutator.submit();
  }

  public static void setFuture(Ample.ConditionalTabletsMutator ctm, KeyExtent extent,
      TServerInstance tsi) {
    Ample.ConditionalTabletMutator conditionalMutator = ctm.mutateTablet(extent);

    conditionalMutator.requireOperation(TabletOperation.NONE);
    conditionalMutator.requireAbsentLocation();
    conditionalMutator.requirePrevEndRow(extent.prevEndRow());

    conditionalMutator.putLocation(tsi, TabletMetadata.LocationType.FUTURE);

    conditionalMutator.submit();
  }

  public static void setCurrent(Ample.ConditionalTabletsMutator ctm, KeyExtent extent,
      TServerInstance tsi) {
    Ample.ConditionalTabletMutator conditionalMutator = ctm.mutateTablet(extent);

    conditionalMutator.requireOperation(TabletOperation.NONE);
    conditionalMutator.requireLocation(tsi, TabletMetadata.LocationType.FUTURE);
    conditionalMutator.requirePrevEndRow(extent.prevEndRow());

    conditionalMutator.putLocation(tsi, TabletMetadata.LocationType.CURRENT);
    conditionalMutator.deleteLocation(tsi, TabletMetadata.LocationType.FUTURE);

    conditionalMutator.submit();
  }

  public static void deleteLocation(Ample.ConditionalTabletsMutator ctm, KeyExtent extent,
      TServerInstance tsi, TabletMetadata.LocationType locType) {
    Ample.ConditionalTabletMutator conditionalMutator = ctm.mutateTablet(extent);

    conditionalMutator.requireOperation(TabletOperation.NONE);
    conditionalMutator.requireLocation(tsi, locType);
    conditionalMutator.requirePrevEndRow(extent.prevEndRow());

    conditionalMutator.deleteLocation(tsi, locType);

    conditionalMutator.submit();
  }

  public static void addTablet(Ample.ConditionalTabletsMutator ctm, KeyExtent extent, String path,
      TimeType timeType) {
    Ample.ConditionalTabletMutator conditionalMutator = ctm.mutateTablet(extent);

    conditionalMutator.requireAbsentTablet();

    conditionalMutator.putPrevEndRow(extent.prevEndRow());
    conditionalMutator.putDirName(path);
    conditionalMutator.putTime(new MetadataTime(0, timeType));
    // TODO used to add lock entry, that can probably go away

    conditionalMutator.submit();
  }

  /**
   * An idempotent metadata operation that can split a tablet into one or more tablets. This method
   * can be called multiple times in the case of process death. Need to pass in the same operation
   * id and same splits when doing this.
   *
   * @param ample A reference to ample
   * @param extent The extent in the metadata table to split
   * @param splits The splits to add inside the extent. Tablets used to only split into two
   *        children. Now that its a metadata only operation, a single tablet can split into
   *        multiple children.
   * @param splitId An id that must be unique for all split operations. This id prevents certain
   *        race conditions like two concurrent operations trying to split the same extent. When
   *        this happens only one will succeed and the other will fail.
   * @throws AccumuloException
   * @throws AccumuloSecurityException
   */
  public static void doSplit(Ample ample, KeyExtent extent, SortedSet<Text> splits,
      OperationId splitId) throws AccumuloException, AccumuloSecurityException {

    if (splits.isEmpty())
      return;

    var result = attemptToReserveTablet(ample, extent, TabletOperation.SPLITTING, splitId);

    if (result.getStatus() == ConditionalWriter.Status.ACCEPTED
        || result.getStatus() == ConditionalWriter.Status.REJECTED
        || result.getStatus() == ConditionalWriter.Status.UNKNOWN) {
      // could be rejected because this operation being retried, will sort everything out when
      // reading the
      // metadata table
    } else {
      throw new IllegalStateException("unexpected status " + result.getStatus());
    }

    // Create the new extents that need to be added to the metadata table
    Set<KeyExtent> newExtents = computeNewExtents(extent, splits);

    // The original extent will be mutated into the following extent
    var lastExtent = new KeyExtent(extent.tableId(), extent.endRow(), splits.last());

    // get the exitsing tablets that fall in the range of the original extent. This method is
    // idempotent so, it possible some of the work was done in a previous call
    Map<KeyExtent,TabletMetadata> existingMetadata = getExistingTablets(ample, extent);

    TabletMetadata primaryTabletMetadata;
    boolean addSplits;

    if (existingMetadata.containsKey(extent)) {
      // the original tablet still exist in the metadata table, so splits have not been added yet
      addSplits = true;
      primaryTabletMetadata = existingMetadata.get(extent);
    } else if (existingMetadata.containsKey(lastExtent)) {
      // The original tablet no longer exists in the metadata table and was mutate into lastExtent.
      // This mutation is done after adding splits, so assuming we do not need to add splits and
      // this subsequent call of this method.
      addSplits = false;
      primaryTabletMetadata = existingMetadata.get(lastExtent);
    } else {
      // no expected extents were seen in the metadata table
      throw new RuntimeException(); // TODO exception and message
    }

    boolean isOperationActive = primaryTabletMetadata.getOperation() == TabletOperation.SPLITTING
        && primaryTabletMetadata.getOperationId().equals(splitId);

    if (!isOperationActive) {
      // The operation and operation id were not observed in the metadata table so can not proceed.
      throw new RuntimeException(); // TODO exception and message
    }

    Collection<StoredTabletFile> files = primaryTabletMetadata.getFiles();

    if (addSplits) {
      // add new tablets to the metadata table
      addNewSplit(ample, splitId, newExtents, existingMetadata, primaryTabletMetadata, files);

      // all new tablets were added, now change the prev row on the original tablet
      updatePrevEndRow(ample, extent, splitId, lastExtent);
    }

    // remove splitting operation from new tablets
    removeSplitOperations(ample, splitId, newExtents);

    // remove splitting state from primary tablet last, this signifies the entire operation is done
    var status =
        removeSplitOperations(ample, splitId, Set.of(lastExtent)).get(lastExtent).getStatus();
    if (status != ConditionalWriter.Status.ACCEPTED) {
      throw new RuntimeException(); // TODO error message and exception
    }
  }

  private static ConditionalWriter.Result attemptToReserveTablet(Ample ample, KeyExtent extent,
      TabletOperation operation, OperationId opid) {

    var tabletMutator = ample.conditionallyMutateTablets().mutateTablet(extent);

    // TODO need to determine in the bigger picture how the tablet will be taken offline and kept
    // offline until the split operation completes
    tabletMutator.requireAbsentLocation();
    tabletMutator.requireOperation(TabletOperation.NONE);
    tabletMutator.requirePrevEndRow(extent.prevEndRow());

    tabletMutator.putOperation(operation);
    tabletMutator.putOperationId(opid);

    return tabletMutator.submit().process().get(extent);
  }

  private static Map<KeyExtent,ConditionalWriter.Result> removeSplitOperations(Ample ample,
      OperationId splitId, Set<KeyExtent> newExtents) {
    var tabletsMutator = ample.conditionallyMutateTablets();
    for (KeyExtent newExtent : newExtents) {
      var tabletMutator = tabletsMutator.mutateTablet(newExtent);

      tabletMutator.requireOperation(TabletOperation.SPLITTING);
      tabletMutator.requireOperationId(splitId);
      tabletMutator.requirePrevEndRow(newExtent.prevEndRow());

      tabletMutator.putOperation(TabletOperation.NONE);
      tabletMutator.deleteOperationId();

      tabletMutator.submit();
    }

    return tabletsMutator.process();
  }

  private static void updatePrevEndRow(Ample ample, KeyExtent extent, OperationId splitId,
      KeyExtent lastExtent) throws AccumuloException, AccumuloSecurityException {
    var tabletMutator = ample.conditionallyMutateTablets().mutateTablet(extent);

    tabletMutator.requireOperation(TabletOperation.SPLITTING);
    tabletMutator.requireOperationId(splitId);
    tabletMutator.requirePrevEndRow(extent.prevEndRow());

    tabletMutator.putPrevEndRow(lastExtent.prevEndRow());
    tabletMutator.submit();

    if (tabletMutator.submit().process().get(extent).getStatus()
        != ConditionalWriter.Status.ACCEPTED) {
      throw new RuntimeException(); // TODO message and exception
    }
  }

  private static void addNewSplit(Ample ample, OperationId splitId, Set<KeyExtent> newExtents,
      Map<KeyExtent,TabletMetadata> existingMetadata, TabletMetadata primaryTabletMetadata,
      Collection<StoredTabletFile> files) {
    var tabletsMutator = ample.conditionallyMutateTablets();
    for (KeyExtent newExtent : newExtents) {
      if (existingMetadata.containsKey(newExtent)) {
        // TODO check things are as expected
        System.out.println("Saw  " + newExtent);
      } else {
        System.out.println("Adding " + newExtent);

        // add a tablet
        var newTabletMutator = tabletsMutator.mutateTablet(newExtent);

        newTabletMutator.requireAbsentTablet();

        // TODO put a dir
        // TODO copy bulk import markers
        // TODO look at current split code to see everything it copies to its children

        newTabletMutator.putOperation(TabletOperation.SPLITTING);
        newTabletMutator.putOperationId(splitId);
        newTabletMutator.putTime(primaryTabletMetadata.getTime());
        newTabletMutator.putPrevEndRow(newExtent.prevEndRow());
        files.forEach(file -> newTabletMutator.putFile(file, null)); // TODO need a data file
                                                                     // value for the file
        newTabletMutator.submit();

      }
    }

    Map<KeyExtent,ConditionalWriter.Result> results = tabletsMutator.process();
    // TODO handle UNKNOWN result
    if (!results.values().stream().allMatch(r -> {
      try {
        return r.getStatus() == ConditionalWriter.Status.ACCEPTED;
      } catch (AccumuloException e) {
        throw new RuntimeException(e);
      } catch (AccumuloSecurityException e) {
        throw new RuntimeException(e);
      }
    })) {
      throw new RuntimeException(); // TODO message and exception
    }
  }

  private static Map<KeyExtent,TabletMetadata> getExistingTablets(Ample ample, KeyExtent extent) {
    Map<KeyExtent,TabletMetadata> existingMetadata = new TreeMap<>();

    // this method should be idempotent, so need ascertain where we are at in the split process
    try (var tablets = ample.readTablets().forTable(extent.tableId())
        .overlapping(extent.prevEndRow(), extent.endRow()).build()) {
      tablets.forEach(
          tabletMetadata -> existingMetadata.put(tabletMetadata.getExtent(), tabletMetadata));
    }
    return existingMetadata;
  }

  private static Set<KeyExtent> computeNewExtents(KeyExtent extent, SortedSet<Text> splits) {
    Set<KeyExtent> newExtents = new HashSet<>();

    Text prev = extent.prevEndRow();
    for (var split : splits) {
      Preconditions.checkArgument(extent.contains(split));
      newExtents.add(new KeyExtent(extent.tableId(), split, prev));
      prev = split;
    }
    return newExtents;
  }
}
