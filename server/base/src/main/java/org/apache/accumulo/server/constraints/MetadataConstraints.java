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
package org.apache.accumulo.server.constraints;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.constraints.Constraint;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ChoppedColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ClonedColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.LogColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ScanFileColumnFamily;
import org.apache.accumulo.core.util.ColumnFQ;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.TransactionWatcher.Arbitrator;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.zookeeper.TransactionWatcher.ZooArbitrator;
import org.apache.accumulo.server.zookeeper.ZooCache;
import org.apache.accumulo.server.zookeeper.ZooLock;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetadataConstraints implements Constraint {

  private ZooCache zooCache = null;
  private String zooRoot = null;

  private static final Logger log = LoggerFactory.getLogger(MetadataConstraints.class);

  private static boolean[] validTableNameChars = new boolean[256];

  {
    for (int i = 0; i < 256; i++) {
      validTableNameChars[i] = ((i >= 'a' && i <= 'z') || (i >= '0' && i <= '9')) || i == '!' || i == '+';
    }
  }

  private static final HashSet<ColumnFQ> validColumnQuals = new HashSet<>(Arrays.asList(TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN,
      TabletsSection.TabletColumnFamily.OLD_PREV_ROW_COLUMN, TabletsSection.SuspendLocationColumn.SUSPEND_COLUMN,
      TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN, TabletsSection.TabletColumnFamily.SPLIT_RATIO_COLUMN, TabletsSection.ServerColumnFamily.TIME_COLUMN,
      TabletsSection.ServerColumnFamily.LOCK_COLUMN, TabletsSection.ServerColumnFamily.FLUSH_COLUMN, TabletsSection.ServerColumnFamily.COMPACT_COLUMN));

  private static final HashSet<Text> validColumnFams = new HashSet<>(Arrays.asList(TabletsSection.BulkFileColumnFamily.NAME, LogColumnFamily.NAME,
      ScanFileColumnFamily.NAME, DataFileColumnFamily.NAME, TabletsSection.CurrentLocationColumnFamily.NAME, TabletsSection.LastLocationColumnFamily.NAME,
      TabletsSection.FutureLocationColumnFamily.NAME, ChoppedColumnFamily.NAME, ClonedColumnFamily.NAME));

  private static boolean isValidColumn(ColumnUpdate cu) {

    if (validColumnFams.contains(new Text(cu.getColumnFamily())))
      return true;

    if (validColumnQuals.contains(new ColumnFQ(cu)))
      return true;

    return false;
  }

  static private ArrayList<Short> addViolation(ArrayList<Short> lst, int violation) {
    if (lst == null)
      lst = new ArrayList<>();
    lst.add((short) violation);
    return lst;
  }

  static private ArrayList<Short> addIfNotPresent(ArrayList<Short> lst, int intViolation) {
    if (lst == null)
      return addViolation(null, intViolation);
    short violation = (short) intViolation;
    if (!lst.contains(violation))
      return addViolation(lst, intViolation);
    return lst;
  }

  @Override
  public List<Short> check(Environment env, Mutation mutation) {

    ArrayList<Short> violations = null;

    Collection<ColumnUpdate> colUpdates = mutation.getUpdates();

    // check the row, it should contains at least one ; or end with <
    boolean containsSemiC = false;

    byte[] row = mutation.getRow();

    // always allow rows that fall within reserved areas
    if (row.length > 0 && row[0] == '~')
      return null;
    if (row.length > 2 && row[0] == '!' && row[1] == '!' && row[2] == '~')
      return null;

    for (byte b : row) {
      if (b == ';') {
        containsSemiC = true;
      }

      if (b == ';' || b == '<')
        break;

      if (!validTableNameChars[0xff & b]) {
        violations = addIfNotPresent(violations, 4);
      }
    }

    if (!containsSemiC) {
      // see if last row char is <
      if (row.length == 0 || row[row.length - 1] != '<') {
        violations = addIfNotPresent(violations, 4);
      }
    } else {
      if (row.length == 0) {
        violations = addIfNotPresent(violations, 4);
      }
    }

    if (row.length > 0 && row[0] == '!') {
      if (row.length < 3 || row[1] != '0' || (row[2] != '<' && row[2] != ';')) {
        violations = addIfNotPresent(violations, 4);
      }
    }

    // ensure row is not less than Constants.METADATA_TABLE_ID
    if (new Text(row).compareTo(new Text(MetadataTable.ID.getUtf8())) < 0) {
      violations = addViolation(violations, 5);
    }

    boolean checkedBulk = false;

    for (ColumnUpdate columnUpdate : colUpdates) {
      Text columnFamily = new Text(columnUpdate.getColumnFamily());

      if (columnUpdate.isDeleted()) {
        if (!isValidColumn(columnUpdate)) {
          violations = addViolation(violations, 2);
        }
        continue;
      }

      if (columnUpdate.getValue().length == 0 && !columnFamily.equals(ScanFileColumnFamily.NAME)) {
        violations = addViolation(violations, 6);
      }

      if (columnFamily.equals(DataFileColumnFamily.NAME)) {
        try {
          DataFileValue dfv = new DataFileValue(columnUpdate.getValue());

          if (dfv.getSize() < 0 || dfv.getNumEntries() < 0) {
            violations = addViolation(violations, 1);
          }
        } catch (NumberFormatException | ArrayIndexOutOfBoundsException nfe) {
          violations = addViolation(violations, 1);
        }
      } else if (columnFamily.equals(ScanFileColumnFamily.NAME)) {

      } else if (columnFamily.equals(TabletsSection.BulkFileColumnFamily.NAME)) {
        if (!columnUpdate.isDeleted() && !checkedBulk) {
          // splits, which also write the time reference, are allowed to write this reference even when
          // the transaction is not running because the other half of the tablet is holding a reference
          // to the file.
          boolean isSplitMutation = false;
          // When a tablet is assigned, it re-writes the metadata. It should probably only update the location information,
          // but it writes everything. We allow it to re-write the bulk information if it is setting the location.
          // See ACCUMULO-1230.
          boolean isLocationMutation = false;

          HashSet<Text> dataFiles = new HashSet<>();
          HashSet<Text> loadedFiles = new HashSet<>();

          String tidString = new String(columnUpdate.getValue(), UTF_8);
          int otherTidCount = 0;

          for (ColumnUpdate update : mutation.getUpdates()) {
            if (new ColumnFQ(update).equals(TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN)) {
              isSplitMutation = true;
            } else if (new Text(update.getColumnFamily()).equals(TabletsSection.CurrentLocationColumnFamily.NAME)) {
              isLocationMutation = true;
            } else if (new Text(update.getColumnFamily()).equals(DataFileColumnFamily.NAME)) {
              dataFiles.add(new Text(update.getColumnQualifier()));
            } else if (new Text(update.getColumnFamily()).equals(TabletsSection.BulkFileColumnFamily.NAME)) {
              loadedFiles.add(new Text(update.getColumnQualifier()));

              if (!new String(update.getValue(), UTF_8).equals(tidString)) {
                otherTidCount++;
              }
            }
          }

          if (!isSplitMutation && !isLocationMutation) {
            long tid = Long.parseLong(tidString);

            try {
              if (otherTidCount > 0 || !dataFiles.equals(loadedFiles) || !getArbitrator().transactionAlive(Constants.BULK_ARBITRATOR_TYPE, tid)) {
                violations = addViolation(violations, 8);
              }
            } catch (Exception ex) {
              violations = addViolation(violations, 8);
            }
          }

          checkedBulk = true;
        }
      } else {
        if (!isValidColumn(columnUpdate)) {
          violations = addViolation(violations, 2);
        } else if (new ColumnFQ(columnUpdate).equals(TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN) && columnUpdate.getValue().length > 0
            && (violations == null || !violations.contains((short) 4))) {
          KeyExtent ke = new KeyExtent(new Text(mutation.getRow()), (Text) null);

          Text per = KeyExtent.decodePrevEndRow(new Value(columnUpdate.getValue()));

          boolean prevEndRowLessThanEndRow = per == null || ke.getEndRow() == null || per.compareTo(ke.getEndRow()) < 0;

          if (!prevEndRowLessThanEndRow) {
            violations = addViolation(violations, 3);
          }
        } else if (new ColumnFQ(columnUpdate).equals(TabletsSection.ServerColumnFamily.LOCK_COLUMN)) {
          if (zooCache == null) {
            zooCache = new ZooCache();
          }

          if (zooRoot == null) {
            zooRoot = ZooUtil.getRoot(HdfsZooInstance.getInstance());
          }

          boolean lockHeld = false;
          String lockId = new String(columnUpdate.getValue(), UTF_8);

          try {
            lockHeld = ZooLock.isLockHeld(zooCache, new ZooUtil.LockID(zooRoot, lockId));
          } catch (Exception e) {
            log.debug("Failed to verify lock was held {} {}", lockId, e.getMessage());
          }

          if (!lockHeld) {
            violations = addViolation(violations, 7);
          }
        }

      }
    }

    if (violations != null) {
      log.debug("violating metadata mutation : {}", new String(mutation.getRow(), UTF_8));
      for (ColumnUpdate update : mutation.getUpdates()) {
        log.debug(" update: {}:{} value {}", new String(update.getColumnFamily(), UTF_8), new String(update.getColumnQualifier(), UTF_8),
            (update.isDeleted() ? "[delete]" : new String(update.getValue(), UTF_8)));
      }
    }

    return violations;
  }

  protected Arbitrator getArbitrator() {
    return new ZooArbitrator();
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
        return "Row can not be less than " + MetadataTable.ID;
      case 6:
        return "Empty values are not allowed for any " + MetadataTable.NAME + " column";
      case 7:
        return "Lock not held in zookeeper by writer";
      case 8:
        return "Bulk load transaction no longer running";
    }
    return null;
  }

  @Override
  protected void finalize() {
    if (zooCache != null)
      zooCache.clear();
  }

}
