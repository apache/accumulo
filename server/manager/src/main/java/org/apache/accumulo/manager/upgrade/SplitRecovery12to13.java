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
package org.apache.accumulo.manager.upgrade;

import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.Upgrade12to13.OLD_PREV_ROW_COLUMN;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.Upgrade12to13.SPLIT_RATIO_COLUMN;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.clientImpl.ScannerImpl;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.util.FileUtil;
import org.apache.accumulo.server.util.MetadataTableUtil;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SplitRecovery12to13 {

  private static final Logger log = LoggerFactory.getLogger(SplitRecovery12to13.class);

  public static KeyExtent fixSplit(ServerContext context, Text metadataEntry)
      throws AccumuloException {
    var tableId = KeyExtent.fromMetaRow(metadataEntry).tableId();

    try (var scanner =
        new ScannerImpl(context, Ample.DataLevel.of(tableId).metaTableId(), Authorizations.EMPTY)) {
      scanner.setRange(new Range(metadataEntry));

      Text oldPrev = null;
      Double persistedSplitRatio = null;
      Text metadataPrevEndRow = null;

      boolean sawOldPrev = false;

      for (var entry : scanner) {
        if (OLD_PREV_ROW_COLUMN.hasColumns(entry.getKey())) {
          oldPrev = TabletsSection.TabletColumnFamily.decodePrevEndRow(entry.getValue());
          sawOldPrev = true;
        } else if (SPLIT_RATIO_COLUMN.hasColumns(entry.getKey())) {
          persistedSplitRatio = Double.parseDouble(entry.getValue().toString());
        } else if (TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.hasColumns(entry.getKey())) {
          metadataPrevEndRow = TabletsSection.TabletColumnFamily.decodePrevEndRow(entry.getValue());
        }
      }

      if (!sawOldPrev || persistedSplitRatio == null || metadataPrevEndRow == null) {
        throw new IllegalStateException("Missing expected columns " + metadataEntry);
      }

      return fixSplit(context, tableId, metadataEntry, metadataPrevEndRow, oldPrev,
          persistedSplitRatio);
    }
  }

  private static KeyExtent fixSplit(ServerContext context, TableId tableId, Text metadataEntry,
      Text metadataPrevEndRow, Text oper, double splitRatio) throws AccumuloException {
    if (metadataPrevEndRow == null) {
      // something is wrong, this should not happen... if a tablet is split, it will always have a
      // prev end row....
      throw new AccumuloException(
          "Split tablet does not have prev end row, something is amiss, extent = " + metadataEntry);
    }

    // check to see if prev tablet exist in metadata tablet
    Key prevRowKey = new Key(new Text(TabletsSection.encodeRow(tableId, metadataPrevEndRow)));

    try (Scanner scanner2 =
        new ScannerImpl(context, Ample.DataLevel.of(tableId).metaTableId(), Authorizations.EMPTY)) {
      scanner2.setRange(new Range(prevRowKey, prevRowKey.followingKey(PartialKey.ROW)));

      if (scanner2.iterator().hasNext()) {
        log.info("Finishing incomplete split {} {}", metadataEntry, metadataPrevEndRow);

        List<StoredTabletFile> highDatafilesToRemove = new ArrayList<>();

        SortedMap<StoredTabletFile,DataFileValue> origDatafileSizes = new TreeMap<>();
        SortedMap<StoredTabletFile,DataFileValue> highDatafileSizes = new TreeMap<>();
        SortedMap<StoredTabletFile,DataFileValue> lowDatafileSizes = new TreeMap<>();

        Key rowKey = new Key(metadataEntry);
        try (Scanner scanner3 = new ScannerImpl(context, Ample.DataLevel.of(tableId).metaTableId(),
            Authorizations.EMPTY)) {

          scanner3.fetchColumnFamily(DataFileColumnFamily.NAME);
          scanner3.setRange(new Range(rowKey, rowKey.followingKey(PartialKey.ROW)));

          for (Entry<Key,Value> entry : scanner3) {
            if (entry.getKey().compareColumnFamily(DataFileColumnFamily.NAME) == 0) {
              StoredTabletFile stf =
                  new StoredTabletFile(entry.getKey().getColumnQualifierData().toString());
              origDatafileSizes.put(stf, new DataFileValue(entry.getValue().get()));
            }
          }
        }

        splitDatafiles(metadataPrevEndRow, splitRatio, new HashMap<>(), origDatafileSizes,
            lowDatafileSizes, highDatafileSizes, highDatafilesToRemove);

        finishSplit(metadataEntry, highDatafileSizes, highDatafilesToRemove, context);

        return KeyExtent.fromMetaRow(rowKey.getRow(), metadataPrevEndRow);
      } else {
        log.info("Rolling back incomplete split {} {}", metadataEntry, metadataPrevEndRow);
        rollBackSplit(metadataEntry, oper, context);
        return KeyExtent.fromMetaRow(metadataEntry, oper);
      }
    }
  }

  public static void splitDatafiles(Text midRow, double splitRatio,
      Map<StoredTabletFile,FileUtil.FileInfo> firstAndLastRows,
      SortedMap<StoredTabletFile,DataFileValue> datafiles,
      SortedMap<StoredTabletFile,DataFileValue> lowDatafileSizes,
      SortedMap<StoredTabletFile,DataFileValue> highDatafileSizes,
      List<StoredTabletFile> highDatafilesToRemove) {

    for (Entry<StoredTabletFile,DataFileValue> entry : datafiles.entrySet()) {

      Text firstRow = null;
      Text lastRow = null;

      boolean rowsKnown = false;

      FileUtil.FileInfo mfi = firstAndLastRows.get(entry.getKey());

      if (mfi != null) {
        firstRow = mfi.getFirstRow();
        lastRow = mfi.getLastRow();
        rowsKnown = true;
      }

      if (rowsKnown && firstRow.compareTo(midRow) > 0) {
        // only in high
        long highSize = entry.getValue().getSize();
        long highEntries = entry.getValue().getNumEntries();
        highDatafileSizes.put(entry.getKey(),
            new DataFileValue(highSize, highEntries, entry.getValue().getTime()));
      } else if (rowsKnown && lastRow.compareTo(midRow) <= 0) {
        // only in low
        long lowSize = entry.getValue().getSize();
        long lowEntries = entry.getValue().getNumEntries();
        lowDatafileSizes.put(entry.getKey(),
            new DataFileValue(lowSize, lowEntries, entry.getValue().getTime()));

        highDatafilesToRemove.add(entry.getKey());
      } else {
        long lowSize = (long) Math.floor((entry.getValue().getSize() * splitRatio));
        long lowEntries = (long) Math.floor((entry.getValue().getNumEntries() * splitRatio));
        lowDatafileSizes.put(entry.getKey(),
            new DataFileValue(lowSize, lowEntries, entry.getValue().getTime()));

        long highSize = (long) Math.ceil((entry.getValue().getSize() * (1.0 - splitRatio)));
        long highEntries =
            (long) Math.ceil((entry.getValue().getNumEntries() * (1.0 - splitRatio)));
        highDatafileSizes.put(entry.getKey(),
            new DataFileValue(highSize, highEntries, entry.getValue().getTime()));
      }
    }
  }

  public static void rollBackSplit(Text metadataEntry, Text oldPrevEndRow, ServerContext context) {
    KeyExtent ke = KeyExtent.fromMetaRow(metadataEntry, oldPrevEndRow);
    Mutation m = TabletsSection.TabletColumnFamily.createPrevRowMutation(ke);
    SPLIT_RATIO_COLUMN.putDelete(m);
    OLD_PREV_ROW_COLUMN.putDelete(m);
    MetadataTableUtil.update(context, null, m, KeyExtent.fromMetaRow(metadataEntry));
  }

  public static void splitTablet(KeyExtent extent, Text oldPrevEndRow, double splitRatio,
      ServerContext context, Set<ExternalCompactionId> ecids) {
    Mutation m = TabletsSection.TabletColumnFamily.createPrevRowMutation(extent);

    SPLIT_RATIO_COLUMN.put(m, new Value(Double.toString(splitRatio)));

    OLD_PREV_ROW_COLUMN.put(m, TabletsSection.TabletColumnFamily.encodePrevEndRow(oldPrevEndRow));

    ecids.forEach(ecid -> m.putDelete(TabletsSection.ExternalCompactionColumnFamily.STR_NAME,
        ecid.canonical()));

    MetadataTableUtil.update(context, null, m, extent);
  }

  public static void finishSplit(Text metadataEntry,
      Map<StoredTabletFile,DataFileValue> datafileSizes,
      List<StoredTabletFile> highDatafilesToRemove, final ServerContext context) {
    Mutation m = new Mutation(metadataEntry);
    SPLIT_RATIO_COLUMN.putDelete(m);
    OLD_PREV_ROW_COLUMN.putDelete(m);

    for (Entry<StoredTabletFile,DataFileValue> entry : datafileSizes.entrySet()) {
      m.put(DataFileColumnFamily.NAME, entry.getKey().getMetadataText(),
          new Value(entry.getValue().encode()));
    }

    for (StoredTabletFile pathToRemove : highDatafilesToRemove) {
      m.putDelete(DataFileColumnFamily.NAME, pathToRemove.getMetadataText());
    }

    MetadataTableUtil.update(context, null, m, KeyExtent.fromMetaRow(metadataEntry));
  }

  public static void finishSplit(KeyExtent extent,
      Map<StoredTabletFile,DataFileValue> datafileSizes,
      List<StoredTabletFile> highDatafilesToRemove, ServerContext context) {
    finishSplit(extent.toMetaRow(), datafileSizes, highDatafilesToRemove, context);
  }

}
