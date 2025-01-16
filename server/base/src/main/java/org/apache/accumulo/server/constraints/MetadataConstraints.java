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

import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.constraints.Constraint;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.metadata.AccumuloTable;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.SuspendingTServer;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.BulkFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ChoppedColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ClonedColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.CurrentLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ExternalCompactionColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.FutureLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.LastLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.LogColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.MergedColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ScanFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.SuspendLocationColumn;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily;
import org.apache.accumulo.core.util.ColumnFQ;
import org.apache.accumulo.server.ServerContext;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetadataConstraints implements Constraint {

  private static final Logger log = LoggerFactory.getLogger(MetadataConstraints.class);
  private static final byte[] BULK_COL_BYTES = BulkFileColumnFamily.STR_NAME.getBytes(UTF_8);

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
          TabletColumnFamily.OLD_PREV_ROW_COLUMN,
          SuspendLocationColumn.SUSPEND_COLUMN,
          ServerColumnFamily.DIRECTORY_COLUMN,
          TabletColumnFamily.SPLIT_RATIO_COLUMN,
          ServerColumnFamily.TIME_COLUMN,
          ServerColumnFamily.LOCK_COLUMN,
          ServerColumnFamily.FLUSH_COLUMN,
          ServerColumnFamily.COMPACT_COLUMN);

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
          CHOPPED,
          MergedColumnFamily.NAME
      );
  // @formatter:on

  private static boolean isValidColumn(ColumnUpdate cu) {

    if (validColumnFams.contains(new Text(cu.getColumnFamily()))) {
      return true;
    }

    return validColumnQuals.contains(new ColumnFQ(cu));
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
      String colFamStr = new String(columnUpdate.getColumnFamily(), UTF_8);

      if (columnUpdate.isDeleted()) {
        if (!isValidColumn(columnUpdate)) {
          addViolation(violations, 2);
        }
        continue;
      }

      validateColValLen(violations, columnUpdate);

      switch (colFamStr) {
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
        default:
          if (!isValidColumn(columnUpdate)) {
            addViolation(violations, 2);
          }
      }
    }

    validateBulkFileFamily(violations, bulkFileColUpdates, bfcValidationData);

    if (!violations.isEmpty()) {
      log.debug("violating metadata mutation : {}", new String(mutation.getRow(), UTF_8));
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
    }
    return null;
  }

  private void validateColValLen(ArrayList<Short> violations, ColumnUpdate columnUpdate) {
    Text columnFamily = new Text(columnUpdate.getColumnFamily());
    if (columnUpdate.getValue().length == 0 && !(columnFamily.equals(ScanFileColumnFamily.NAME)
        || columnFamily.equals(LogColumnFamily.NAME))) {
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

    if (qualStr.equals(TabletColumnFamily.PREV_ROW_QUAL)) {
      if (columnUpdate.getValue().length > 0 && !violations.contains((short) 4)) {
        KeyExtent ke = KeyExtent.fromMetaRow(new Text(mutation.getRow()));
        Text per = TabletColumnFamily.decodePrevEndRow(new Value(columnUpdate.getValue()));
        boolean prevEndRowLessThanEndRow =
            per == null || ke.endRow() == null || per.compareTo(ke.endRow()) < 0;

        if (!prevEndRowLessThanEndRow) {
          addViolation(violations, 3);
        }
      }
    }
  }

  private void validateServerFamily(ArrayList<Short> violations, ColumnUpdate columnUpdate,
      ServerContext context, BulkFileColData bfcValidationData) {
    String qualStr = new String(columnUpdate.getColumnQualifier(), UTF_8);

    switch (qualStr) {
      case ServerColumnFamily.LOCK_QUAL:
        boolean lockHeld = false;
        String lockId = new String(columnUpdate.getValue(), UTF_8);

        try {
          lockHeld = ServiceLock.isLockHeld(context.getZooCache(),
              new ZooUtil.LockID(context.getZooKeeperRoot(), lockId));
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
          addViolation(violations, 3102);
        }

        // splits, which also write the time reference, are allowed to write this reference
        // even when the transaction is not running because the other half of the tablet is
        // holding a reference to the file.
        if (bfcValidationData != null) {
          bfcValidationData.setIsSplitMutation(true);
        }
        break;
    }
  }

  private void validateSuspendLocationFamily(ArrayList<Short> violations,
      ColumnUpdate columnUpdate) {
    String qualStr = new String(columnUpdate.getColumnQualifier(), UTF_8);
    String suspendColQualStr = SuspendLocationColumn.SUSPEND_QUAL;

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
        if (bfcValidationData.getTidsSeen().size() > 1
            || !bfcValidationData.dataFilesEqualsLoadedFiles()) {
          addViolation(violations, 8);
        }
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
