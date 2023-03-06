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

  public void compact(Ample.ConditionalTabletsMutator ctm, KeyExtent extent,
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

  public void minorCompact(Ample.ConditionalTabletsMutator ctm, KeyExtent extent,
      TServerInstance tsi, TabletFile newFile, DataFileValue dfv) {
    Ample.ConditionalTabletMutator conditionalMutator = ctm.mutateTablet(extent);

    // when a tablet has a current location its state should always be nominal, so this check is a
    // bit redundant
    conditionalMutator.requireOperation(TabletOperation.NONE);
    conditionalMutator.requireLocation(tsi, TabletMetadata.LocationType.CURRENT);
    conditionalMutator.requirePrevEndRow(extent.prevEndRow());
    // TODO could add a conditional check to ensure file is not already there

    conditionalMutator.putFile(newFile, dfv);

    conditionalMutator.submit();
  }

  public void bulkImport(Ample.ConditionalTabletsMutator ctm, KeyExtent extent,
      Map<TabletFile,DataFileValue> newFiles) {
    Ample.ConditionalTabletMutator conditionalMutator = ctm.mutateTablet(extent);

    conditionalMutator.requireOperation(TabletOperation.NONE);
    conditionalMutator.requirePrevEndRow(extent.prevEndRow());
    //TODO check bulk import status of file in tablet metadata... do not want to reimport a file that was previously imported and compacted away

    newFiles.forEach(conditionalMutator::putFile);

    conditionalMutator.submit();
  }

  public void setFuture(Ample.ConditionalTabletsMutator ctm, KeyExtent extent,
      TServerInstance tsi) {
    Ample.ConditionalTabletMutator conditionalMutator = ctm.mutateTablet(extent);

    conditionalMutator.requireOperation(TabletOperation.NONE);
    conditionalMutator.requireAbsentLocation();
    conditionalMutator.requirePrevEndRow(extent.prevEndRow());

    conditionalMutator.putLocation(tsi, TabletMetadata.LocationType.FUTURE);

    conditionalMutator.submit();
  }

  public void setCurrent(Ample.ConditionalTabletsMutator ctm, KeyExtent extent,
      TServerInstance tsi) {
    Ample.ConditionalTabletMutator conditionalMutator = ctm.mutateTablet(extent);

    conditionalMutator.requireOperation(TabletOperation.NONE);
    conditionalMutator.requireLocation(tsi, TabletMetadata.LocationType.FUTURE);
    conditionalMutator.requirePrevEndRow(extent.prevEndRow());

    conditionalMutator.putLocation(tsi, TabletMetadata.LocationType.CURRENT);
    conditionalMutator.deleteLocation(tsi, TabletMetadata.LocationType.FUTURE);

    conditionalMutator.submit();
  }

  public void deleteLocation(Ample.ConditionalTabletsMutator ctm, KeyExtent extent,
      TServerInstance tsi, TabletMetadata.LocationType locType) {
    Ample.ConditionalTabletMutator conditionalMutator = ctm.mutateTablet(extent);

    conditionalMutator.requireOperation(TabletOperation.NONE);
    conditionalMutator.requireLocation(tsi, locType);
    conditionalMutator.requirePrevEndRow(extent.prevEndRow());

    conditionalMutator.deleteLocation(tsi, locType);

    conditionalMutator.submit();
  }

  public void addTablet(Ample.ConditionalTabletsMutator ctm, KeyExtent extent, String path,
      TimeType timeType) {
    Ample.ConditionalTabletMutator conditionalMutator = ctm.mutateTablet(extent);

    conditionalMutator.requireAbsentTablet();

    conditionalMutator.putPrevEndRow(extent.prevEndRow());
    conditionalMutator.putDirName(path);
    conditionalMutator.putTime(new MetadataTime(0, timeType));
    // TODO used to add lock entry, that can probably go away

    conditionalMutator.submit();
  }

  // Tablets used to only split into two children. Now that its a metadata only operation, a single
  // tablet can split into multiple children.
  public void doSplit(Ample ample, KeyExtent extent, SortedSet<Text> splits, OperationId splitId)
      throws AccumuloException, AccumuloSecurityException {

    var tabletsMutator = ample.conditionallyMutateTablets();

    var tabletMutator = tabletsMutator.mutateTablet(extent);

    //TODO need to determine in the bigger picture how the tablet will be taken offline and kept offline until the split operation completes
    tabletMutator.requireAbsentLocation();
    tabletMutator.requireOperation(TabletOperation.NONE);
    tabletMutator.requirePrevEndRow(extent.prevEndRow());

    tabletMutator.putOperation(TabletOperation.SPLITTING);
    tabletMutator.putOperationId(splitId);

    tabletMutator.submit();

    var result = tabletsMutator.process().get(extent);
    if (result.getStatus() == ConditionalWriter.Status.ACCEPTED
        || result.getStatus() == ConditionalWriter.Status.REJECTED
        || result.getStatus() == ConditionalWriter.Status.UNKNOWN) {
      // could be rejected because this is being recalled, will sort everything out when reading the
      // metadata table
    } else {
      throw new IllegalStateException("unexpected status " + result.getStatus());
    }

    Set<KeyExtent> newExtents = new HashSet<>();

    Text prev = extent.prevEndRow();
    for (var split : splits) {
      Preconditions.checkArgument(extent.contains(split));
      newExtents.add(new KeyExtent(extent.tableId(), split, prev));
      prev = split;
    }

    var lastExtent = new KeyExtent(extent.tableId(), extent.endRow(), prev);

    Map<KeyExtent,TabletMetadata> existingMetadata = new TreeMap<>();

    // this method should be idempotent, so need ascertain where we are at in the split process
    try (var tablets = ample.readTablets().forTable(extent.tableId())
        .overlapping(extent.prevEndRow(), extent.endRow()).build()) {
      tablets.forEach(
          tabletMetadata -> existingMetadata.put(tabletMetadata.getExtent(), tabletMetadata));
    }

    TabletMetadata primaryTabletMetadata;
    boolean addSplits;

    if (existingMetadata.containsKey(extent)) {
      addSplits = true;
      primaryTabletMetadata = existingMetadata.get(extent);
    } else if (existingMetadata.containsKey(lastExtent)) {
      // the split operation changes the prev end row after all the splits are added, so since the
      // prev row has been changed this indicates the splits were already added
      addSplits = false;
      primaryTabletMetadata = existingMetadata.get(lastExtent);
    } else {
      // no expected extents were seen in the metadata table
      throw new RuntimeException(); // TODO exception and message
    }

    boolean isOperationActive = primaryTabletMetadata.getOperation() == TabletOperation.SPLITTING
        && primaryTabletMetadata.getOperationId().equals(splitId);
    if (!isOperationActive) {
      // tablet is not in an expected state to start making changes
      throw new RuntimeException(); // TODO exception and message
    }

    Collection<StoredTabletFile> files = primaryTabletMetadata.getFiles();

    if (addSplits) {
      tabletsMutator = ample.conditionallyMutateTablets();
      for (KeyExtent newExtent : newExtents) {
        if (existingMetadata.containsKey(extent)) {
          // TODO check things are as expected
        } else {
          // add a tablet
          var newTabletMutator = tabletsMutator.mutateTablet(newExtent);

          newTabletMutator.requireAbsentTablet();

          // TODO put a dir
          // TODO copy bulk import markers
          // TODO look at current split code to see everything it copies toc hildren

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

      // all new tablets were added, now change the prev row on the primary tablet
      tabletsMutator = ample.conditionallyMutateTablets();
      tabletMutator = tabletsMutator.mutateTablet(extent);

      tabletMutator.requireOperation(TabletOperation.SPLITTING);
      tabletMutator.requireOperationId(splitId);
      tabletMutator.requirePrevEndRow(extent.prevEndRow());

      tabletMutator.putPrevEndRow(lastExtent.prevEndRow());
      tabletMutator.submit();

      if (tabletsMutator.process().get(extent).getStatus() != ConditionalWriter.Status.ACCEPTED) {
        throw new RuntimeException(); // TODO message and exception
      }
    }

    // remove splitting state from new tablets
    tabletsMutator = ample.conditionallyMutateTablets();
    for (KeyExtent newExtent : newExtents) {
      tabletMutator = tabletsMutator.mutateTablet(newExtent);

      tabletMutator.requireOperation(TabletOperation.SPLITTING);
      tabletMutator.requireOperationId(splitId);
      tabletMutator.requirePrevEndRow(extent.prevEndRow());

      tabletMutator.putOperation(TabletOperation.NONE);
      tabletMutator.deleteOperationId();

      tabletMutator.submit();
    }

    // TODO check status... may only need to debug in the failure and rerun case some tablets could
    // change after their operation is unset
    tabletsMutator.process();

    // remove splitting state from primary tablet last
    tabletsMutator = ample.conditionallyMutateTablets();
    tabletMutator = tabletsMutator.mutateTablet(lastExtent);

    tabletMutator.requireOperation(TabletOperation.SPLITTING);
    tabletMutator.requireOperationId(splitId);
    tabletMutator.requirePrevEndRow(extent.prevEndRow());

    tabletMutator.putOperation(TabletOperation.NONE);
    tabletMutator.deleteOperationId();
    tabletMutator.submit();

    var status = tabletsMutator.process().get(lastExtent).getStatus();
    if (status != ConditionalWriter.Status.ACCEPTED) {
      throw new RuntimeException(); // TODO error message and exception
    }
  }
}
