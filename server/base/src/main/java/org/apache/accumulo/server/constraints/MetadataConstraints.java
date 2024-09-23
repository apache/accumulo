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
package org.apache.accumulo.server.constraints;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.accumulo.core.clientImpl.TabletAvailabilityUtil;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.constraints.Constraint;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.zookeeper.ZooCache;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.metadata.AccumuloTable;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.SuspendingTServer;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.BulkFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ChoppedColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ClonedColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.CompactedColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.CurrentLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ExternalCompactionColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.FutureLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.LastLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.LogColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.MergedColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ScanFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.SplitColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.SuspendLocationColumn;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.Upgrade12to13;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.UserCompactionRequestedColumnFamily;
import org.apache.accumulo.core.metadata.schema.SelectedFiles;
import org.apache.accumulo.core.metadata.schema.TabletOperationId;
import org.apache.accumulo.core.metadata.schema.TabletOperationType;
import org.apache.accumulo.core.metadata.schema.UnSplittableMetadata;
import org.apache.accumulo.core.util.ColumnFQ;
import org.apache.accumulo.core.util.cleaner.CleanerUtil;
import org.apache.accumulo.server.ServerContext;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetadataConstraints implements Constraint {

  private static final Logger log = LoggerFactory.getLogger(MetadataConstraints.class);
  private static final byte[] BULK_COL_BYTES = BulkFileColumnFamily.STR_NAME.getBytes(UTF_8);

  private ZooCache zooCache = null;
  private String zooRoot = null;

  private static final boolean[] validTableNameChars = new boolean[256];
  static {
    for (int i = 0; i < 256; i++) {
      validTableNameChars[i] =
          ((i >= 'a' && i <= 'z') || (i >= '0' && i <= '9')) || i == '!' || i == '+';
    }
  }

  // @formatter:off
  private static final Set<ColumnFQ> validColumnQuals =
      Set.of(TabletColumnFamily.PREV_ROW_COLUMN,
          Upgrade12to13.OLD_PREV_ROW_COLUMN,
          SuspendLocationColumn.SUSPEND_COLUMN,
          ServerColumnFamily.DIRECTORY_COLUMN,
          Upgrade12to13.SPLIT_RATIO_COLUMN,
          ServerColumnFamily.TIME_COLUMN,
          ServerColumnFamily.LOCK_COLUMN,
          ServerColumnFamily.FLUSH_COLUMN,
          ServerColumnFamily.FLUSH_NONCE_COLUMN,
          ServerColumnFamily.OPID_COLUMN,
          TabletColumnFamily.AVAILABILITY_COLUMN,
          TabletColumnFamily.REQUESTED_COLUMN,
          ServerColumnFamily.SELECTED_COLUMN,
          SplitColumnFamily.UNSPLITTABLE_COLUMN,
          Upgrade12to13.COMPACT_COL);

  @SuppressWarnings("deprecation")
  private static final Text CHOPPED = ChoppedColumnFamily.NAME;

  private static final Set<Text> validColumnFams =
      Set.of(BulkFileColumnFamily.NAME,
          LogColumnFamily.NAME,
          ScanFileColumnFamily.NAME,
          DataFileColumnFamily.NAME,
          CurrentLocationColumnFamily.NAME,
          LastLocationColumnFamily.NAME,
          FutureLocationColumnFamily.NAME,
          ClonedColumnFamily.NAME,
          ExternalCompactionColumnFamily.NAME,
          CompactedColumnFamily.NAME,
          CHOPPED,
          MergedColumnFamily.NAME,
          UserCompactionRequestedColumnFamily.NAME
      );
  // @formatter:on

  private static boolean isValidColumn(Text family, Text qualifier) {

    if (validColumnFams.contains(family)) {
      return true;
    }

    return validColumnQuals.contains(new ColumnFQ(family, qualifier));
  }

  private static void addViolation(ArrayList<Short> lst, int violation) {
    lst.add((short) violation);
  }

  private static void addIfNotPresent(ArrayList<Short> lst, int violation) {
    if (!lst.contains((short) violation)) {
      addViolation(lst, violation);
    }
  }

  /*
   * Validates the data file metadata is valid for a StoredTabletFile.
   */
  private static void validateDataFileMetadata(ArrayList<Short> violations, String metadata,
      Consumer<StoredTabletFile> stfConsumer) {
    try {
      stfConsumer.accept(StoredTabletFile.of(metadata));
    } catch (RuntimeException e) {
      addViolation(violations, 3100);
    }
  }

  /**
   * Same as defined in {@link Constraint#check(Environment, Mutation)}, but returns an empty list
   * instead of null if there are no violations
   *
   * @param env constraint environment
   * @param mutation mutation to check
   * @return list of violations, or empty list if no violations
   */
  @Override
  public List<Short> check(Environment env, Mutation mutation) {
    final ServerContext context = ((SystemEnvironment) env).getServerContext();
    final ArrayList<Short> violations = new ArrayList<>();
    final Collection<ColumnUpdate> colUpdates = mutation.getUpdates();
    final byte[] row = mutation.getRow();
    final List<ColumnUpdate> bulkFileColUpdates;
    final BulkFileColData bfcValidationData;

    // avoids creating unnecessary objects
    if (hasBulkCol(colUpdates)) {
      bulkFileColUpdates = new ArrayList<>();
      bfcValidationData = new BulkFileColData();
    } else {
      bulkFileColUpdates = null;
      bfcValidationData = null;
    }

    // always allow rows that fall within reserved areas
    if (row.length > 0 && row[0] == '~') {
      return violations;
    }

    validateTabletRow(violations, row);

    for (ColumnUpdate columnUpdate : colUpdates) {
      Text columnFamily = new Text(columnUpdate.getColumnFamily());
      Text columnQualifier = new Text(columnUpdate.getColumnQualifier());

      if (columnUpdate.isDeleted()) {
        if (!isValidColumn(columnFamily, columnQualifier)) {
          addViolation(violations, 2);
        }
        continue;
      }

      validateColValLen(violations, columnUpdate);

      switch (columnFamily.toString()) {
        case TabletColumnFamily.STR_NAME:
          validateTabletFamily(violations, columnUpdate, mutation);
          break;
        case ServerColumnFamily.STR_NAME:
          validateServerFamily(violations, columnUpdate, context, bfcValidationData);
          break;
        case CurrentLocationColumnFamily.STR_NAME:
          // When a tablet is assigned, it re-writes the metadata. It should probably only update
          // the location information, but it writes everything. We allow it to re-write the bulk
          // information if it is setting the location.
          // See ACCUMULO-1230.
          if (bfcValidationData != null) {
            bfcValidationData.setIsLocationMutation(true);
          }
          break;
        case SuspendLocationColumn.STR_NAME:
          validateSuspendLocationFamily(violations, columnUpdate);
          break;
        case BulkFileColumnFamily.STR_NAME:
          // defer validating the bulk file column updates until the end (relies on checks done
          // on other column updates)
          if (bulkFileColUpdates != null) {
            bulkFileColUpdates.add(columnUpdate);
          }
          break;
        case DataFileColumnFamily.STR_NAME:
          validateDataFileFamily(violations, columnUpdate, bfcValidationData);
          break;
        case ScanFileColumnFamily.STR_NAME:
          validateScanFileFamily(violations, columnUpdate);
          break;
        case CompactedColumnFamily.STR_NAME:
          validateCompactedFamily(violations, columnUpdate);
          break;
        case UserCompactionRequestedColumnFamily.STR_NAME:
          validateUserCompactionRequestedFamily(violations, columnUpdate);
          break;
        case SplitColumnFamily.STR_NAME:
          validateSplitFamily(violations, columnUpdate);
          break;
        default:
          if (!isValidColumn(columnFamily, columnQualifier)) {
            addViolation(violations, 2);
          }
      }
    }

    validateBulkFileFamily(violations, bulkFileColUpdates, bfcValidationData);

    if (!violations.isEmpty()) {
      log.debug("violating metadata mutation : {} {}", new String(mutation.getRow(), UTF_8),
          violations);
      for (ColumnUpdate update : mutation.getUpdates()) {
        log.debug(" update: {}:{} value {}", new String(update.getColumnFamily(), UTF_8),
            new String(update.getColumnQualifier(), UTF_8),
            (update.isDeleted() ? "[delete]" : new String(update.getValue(), UTF_8)));
      }
    }

    return violations;
  }

  @Override
  public String getViolationDescription(short violationCode) {
    switch (violationCode) {
      case 1:
        return "data file size must be a non-negative integer";
      case 2:
        return "Invalid column name given.";
      case 3:
        return "Prev end row is greater than or equal to end row.";
      case 4:
        return "Invalid metadata row format";
      case 5:
        return "Row can not be less than " + AccumuloTable.METADATA.tableId();
      case 6:
        return "Empty values are not allowed for any " + AccumuloTable.METADATA.tableName()
            + " column";
      case 7:
        return "Lock not held in zookeeper by writer";
      case 8:
        return "Bulk load mutation contains either inconsistent files or multiple fateTX ids";
      case 3100:
        return "Invalid data file metadata format";
      case 3101:
        return "Suspended timestamp is not valid";
      case 3102:
        return "Invalid directory column value";
      case 4000:
        return "Malformed operation id";
      case 4001:
        return "Malformed file selection value";
      case 4002:
        return "Invalid compacted column";
      case 4003:
        return "Invalid user compaction requested column";
      case 4004:
        return "Invalid unsplittable column";
      case 4005:
        return "Malformed availability value";

    }
    return null;
  }

  private void validateColValLen(ArrayList<Short> violations, ColumnUpdate columnUpdate) {
    Text columnFamily = new Text(columnUpdate.getColumnFamily());
    Text columnQualifier = new Text(columnUpdate.getColumnQualifier());
    if (columnUpdate.getValue().length == 0 && !(columnFamily.equals(ScanFileColumnFamily.NAME)
        || columnFamily.equals(LogColumnFamily.NAME)
        || TabletColumnFamily.REQUESTED_COLUMN.equals(columnFamily, columnQualifier)
        || columnFamily.equals(CompactedColumnFamily.NAME)
        || columnFamily.equals(UserCompactionRequestedColumnFamily.NAME))) {
      addViolation(violations, 6);
    }
  }

  private void validateTabletRow(ArrayList<Short> violations, byte[] row) {
    // check the row, it should contain at least one ";" or end with "<". Row should also
    // not be less than AccumuloTable.METADATA.tableId().
    boolean containsSemiC = false;

    for (byte b : row) {
      if (b == ';') {
        containsSemiC = true;
      }

      if (b == ';' || b == '<') {
        break;
      }

      if (!validTableNameChars[0xff & b]) {
        addIfNotPresent(violations, 4);
      }
    }

    if (!containsSemiC) {
      // see if last row char is <
      if (row.length == 0 || row[row.length - 1] != '<') {
        addIfNotPresent(violations, 4);
      }
    }

    if (row.length > 0 && row[0] == '!') {
      if (row.length < 3 || row[1] != '0' || (row[2] != '<' && row[2] != ';')) {
        addIfNotPresent(violations, 4);
      }
    }

    // ensure row is not less than AccumuloTable.METADATA.tableId()
    if (Arrays.compare(row, AccumuloTable.METADATA.tableId().canonical().getBytes(UTF_8)) < 0) {
      addViolation(violations, 5);
    }
  }

  private void validateTabletFamily(ArrayList<Short> violations, ColumnUpdate columnUpdate,
      Mutation mutation) {
    String qualStr = new String(columnUpdate.getColumnQualifier(), UTF_8);

    switch (qualStr) {
      case (TabletColumnFamily.PREV_ROW_QUAL):
        if (columnUpdate.getValue().length > 0 && !violations.contains((short) 4)) {
          KeyExtent ke = KeyExtent.fromMetaRow(new Text(mutation.getRow()));
          Text per = TabletColumnFamily.decodePrevEndRow(new Value(columnUpdate.getValue()));
          boolean prevEndRowLessThanEndRow =
              per == null || ke.endRow() == null || per.compareTo(ke.endRow()) < 0;

          if (!prevEndRowLessThanEndRow) {
            addViolation(violations, 3);
          }
        }
        break;
      case (TabletColumnFamily.AVAILABILITY_QUAL):
        try {
          TabletAvailabilityUtil.fromValue(new Value(columnUpdate.getValue()));
        } catch (IllegalArgumentException e) {
          addViolation(violations, 4005);
        }
        break;
    }
  }

  private void validateServerFamily(ArrayList<Short> violations, ColumnUpdate columnUpdate,
      ServerContext context, BulkFileColData bfcValidationData) {
    String qualStr = new String(columnUpdate.getColumnQualifier(), UTF_8);

    switch (qualStr) {
      case ServerColumnFamily.LOCK_QUAL:
        if (zooCache == null) {
          zooCache = new ZooCache(context.getZooReader(), null);
          CleanerUtil.zooCacheClearer(this, zooCache);
        }

        if (zooRoot == null) {
          zooRoot = context.getZooKeeperRoot();
        }

        boolean lockHeld = false;
        String lockId = new String(columnUpdate.getValue(), UTF_8);

        try {
          lockHeld = ServiceLock.isLockHeld(zooCache, new ZooUtil.LockID(zooRoot, lockId));
        } catch (Exception e) {
          log.debug("Failed to verify lock was held {} {}", lockId, e.getMessage());
        }

        if (!lockHeld) {
          addViolation(violations, 7);
        }
        break;
      case ServerColumnFamily.DIRECTORY_QUAL:
        try {
          ServerColumnFamily.validateDirCol(new String(columnUpdate.getValue(), UTF_8));
        } catch (IllegalArgumentException e) {
          addViolation(violations, (short) 3102);
        }
        break;
      case ServerColumnFamily.OPID_QUAL:
        try {
          // Loading the TabletOperationId will also validate it
          var id = TabletOperationId.from(new String(columnUpdate.getValue(), UTF_8));
          if (id.getType() == TabletOperationType.SPLITTING) {
            // splits, which also write the time reference, are allowed to write this reference
            // even when the transaction is not running because the other half of the tablet is
            // holding a reference to the file.
            if (bfcValidationData != null) {
              bfcValidationData.setIsSplitMutation(true);
            }
          }
        } catch (IllegalArgumentException e) {
          addViolation(violations, 4000);
        }
        break;
      case ServerColumnFamily.SELECTED_QUAL:
        try {
          SelectedFiles.from(new String(columnUpdate.getValue(), UTF_8));
        } catch (RuntimeException e) {
          addViolation(violations, 4001);
        }
        break;
    }
  }

  private void validateSuspendLocationFamily(ArrayList<Short> violations,
      ColumnUpdate columnUpdate) {
    String qualStr = new String(columnUpdate.getColumnQualifier(), UTF_8);
    String suspendColQualStr =
        new String(SuspendLocationColumn.SUSPEND_COLUMN.getColumnQualifier().getBytes(), UTF_8);

    if (qualStr.equals(suspendColQualStr)) {
      try {
        SuspendingTServer.fromValue(new Value(columnUpdate.getValue()));
      } catch (IllegalArgumentException e) {
        addViolation(violations, 3101);
      }
    }
  }

  private void validateDataFileFamily(ArrayList<Short> violations, ColumnUpdate columnUpdate,
      BulkFileColData bfcValidationData) {
    Consumer<StoredTabletFile> stfConsumer =
        bfcValidationData == null ? stf -> {} : bfcValidationData::addDataFile;
    validateDataFileMetadata(violations, new String(columnUpdate.getColumnQualifier(), UTF_8),
        stfConsumer);

    try {
      DataFileValue dfv = new DataFileValue(columnUpdate.getValue());

      if (dfv.getSize() < 0 || dfv.getNumEntries() < 0) {
        addViolation(violations, 1);
      }
    } catch (NumberFormatException | ArrayIndexOutOfBoundsException nfe) {
      addViolation(violations, 1);
    }
  }

  private void validateScanFileFamily(ArrayList<Short> violations, ColumnUpdate columnUpdate) {
    validateDataFileMetadata(violations, new String(columnUpdate.getColumnQualifier(), UTF_8),
        stf -> {});
  }

  private void validateBulkFileFamily(ArrayList<Short> violations,
      Collection<ColumnUpdate> bulkFileColUpdates, BulkFileColData bfcValidationData) {
    if (bulkFileColUpdates != null && !bulkFileColUpdates.isEmpty()) {
      for (ColumnUpdate bulkFileColUpdate : bulkFileColUpdates) {
        validateDataFileMetadata(violations,
            new String(bulkFileColUpdate.getColumnQualifier(), UTF_8),
            bfcValidationData::addLoadedFile);

        bfcValidationData.addTidSeen(new String(bulkFileColUpdate.getValue(), UTF_8));
      }

      if (!bfcValidationData.getIsSplitMutation() && !bfcValidationData.getIsLocationMutation()) {
        for (String tidString : bfcValidationData.getTidsSeen()) {
          try {
            // attempt to parse value
            BulkFileColumnFamily.getBulkLoadTid(new Value(tidString));
          } catch (Exception e) {
            addViolation(violations, 8);
          }
        }
        if (bfcValidationData.getTidsSeen().size() > 1
            || !bfcValidationData.dataFilesEqualsLoadedFiles()) {
          addViolation(violations, 8);
        }
      }
    }
  }

  private void validateCompactedFamily(ArrayList<Short> violations, ColumnUpdate columnUpdate) {
    if (!FateId.isFateId(new String(columnUpdate.getColumnQualifier(), UTF_8))) {
      addViolation(violations, 4002);
    }
  }

  private void validateUserCompactionRequestedFamily(ArrayList<Short> violations,
      ColumnUpdate columnUpdate) {
    if (!FateId.isFateId(new String(columnUpdate.getColumnQualifier(), UTF_8))) {
      addViolation(violations, 4003);
    }
  }

  private void validateSplitFamily(ArrayList<Short> violations, ColumnUpdate columnUpdate) {
    String qualStr = new String(columnUpdate.getColumnQualifier(), UTF_8);

    if (qualStr.equals(SplitColumnFamily.UNSPLITTABLE_QUAL)) {
      try {
        UnSplittableMetadata.toUnSplittable(new String(columnUpdate.getValue(), UTF_8));
      } catch (RuntimeException e) {
        addViolation(violations, 4004);
      }
    }
  }

  private boolean hasBulkCol(Collection<ColumnUpdate> colUpdates) {
    for (ColumnUpdate colUpdate : colUpdates) {
      if (Arrays.equals(colUpdate.getColumnFamily(), BULK_COL_BYTES)) {
        return true;
      }
    }
    return false;
  }

}
