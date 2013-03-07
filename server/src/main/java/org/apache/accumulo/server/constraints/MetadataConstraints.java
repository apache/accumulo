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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.constraints.Constraint;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.ColumnFQ;
import org.apache.accumulo.core.util.MetadataTable.DataFileValue;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.zookeeper.ZooCache;
import org.apache.accumulo.server.zookeeper.ZooLock;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

public class MetadataConstraints implements Constraint {
  
  private ZooCache zooCache = null;
  private String zooRoot = null;
  
  private static final Logger log = Logger.getLogger(MetadataConstraints.class);
  
  private static boolean[] validTableNameChars = new boolean[256];
  
  {
    for (int i = 0; i < 256; i++) {
      validTableNameChars[i] = ((i >= 'a' && i <= 'z') || (i >= '0' && i <= '9')) || i == '!';
    }
  }
  
  private static final HashSet<ColumnFQ> validColumnQuals = new HashSet<ColumnFQ>(Arrays.asList(new ColumnFQ[] {Constants.METADATA_PREV_ROW_COLUMN,
      Constants.METADATA_OLD_PREV_ROW_COLUMN, Constants.METADATA_DIRECTORY_COLUMN, Constants.METADATA_SPLIT_RATIO_COLUMN, Constants.METADATA_TIME_COLUMN,
      Constants.METADATA_LOCK_COLUMN, Constants.METADATA_FLUSH_COLUMN, Constants.METADATA_COMPACT_COLUMN}));
  
  private static final HashSet<Text> validColumnFams = new HashSet<Text>(Arrays.asList(new Text[] {Constants.METADATA_BULKFILE_COLUMN_FAMILY,
      Constants.METADATA_LOG_COLUMN_FAMILY, Constants.METADATA_SCANFILE_COLUMN_FAMILY, Constants.METADATA_DATAFILE_COLUMN_FAMILY,
      Constants.METADATA_CURRENT_LOCATION_COLUMN_FAMILY, Constants.METADATA_LAST_LOCATION_COLUMN_FAMILY, Constants.METADATA_FUTURE_LOCATION_COLUMN_FAMILY,
      Constants.METADATA_CHOPPED_COLUMN_FAMILY, Constants.METADATA_CLONED_COLUMN_FAMILY}));
  
  private static boolean isValidColumn(ColumnUpdate cu) {
    
    if (validColumnFams.contains(new Text(cu.getColumnFamily())))
      return true;
    
    if (validColumnQuals.contains(new ColumnFQ(cu)))
      return true;
    
    return false;
  }
  
  public List<Short> check(Environment env, Mutation mutation) {
    
    ArrayList<Short> violations = null;
    
    Collection<ColumnUpdate> colUpdates = mutation.getUpdates();
    
    // check the row, it should contains at least one ; or end with <
    boolean containsSemiC = false;
    
    byte[] row = mutation.getRow();
    
    // always allow rows that fall within reserved area
    if (row.length > 0 && row[0] == '~')
      return null;
    
    for (byte b : row) {
      if (b == ';') {
        containsSemiC = true;
      }
      
      if (b == ';' || b == '<')
        break;
      
      if (!validTableNameChars[0xff & b]) {
        if (violations == null)
          violations = new ArrayList<Short>();
        if (!violations.contains((short) 4))
          violations.add((short) 4);
      }
    }
    
    if (!containsSemiC) {
      // see if last row char is <
      if (row.length == 0 || row[row.length - 1] != '<') {
        if (violations == null)
          violations = new ArrayList<Short>();
        if (!violations.contains((short) 4))
          violations.add((short) 4);
      }
    } else {
      if (row.length == 0) {
        if (violations == null)
          violations = new ArrayList<Short>();
        if (!violations.contains((short) 4))
          violations.add((short) 4);
      }
    }
    
    if (row.length > 0 && row[0] == '!') {
      if (row.length < 3 || row[1] != '0' || (row[2] != '<' && row[2] != ';')) {
        if (violations == null)
          violations = new ArrayList<Short>();
        if (!violations.contains((short) 4))
          violations.add((short) 4);
      }
    }
    
    // ensure row is not less than Constants.METADATA_TABLE_ID
    if (new Text(row).compareTo(new Text(Constants.METADATA_TABLE_ID)) < 0) {
      if (violations == null)
        violations = new ArrayList<Short>();
      violations.add((short) 5);
    }
    
    for (ColumnUpdate columnUpdate : colUpdates) {
      Text columnFamily = new Text(columnUpdate.getColumnFamily());
      
      if (columnUpdate.isDeleted()) {
        if (!isValidColumn(columnUpdate)) {
          if (violations == null)
            violations = new ArrayList<Short>();
          violations.add((short) 2);
        }
        continue;
      }
      
      if (columnUpdate.getValue().length == 0 && !columnFamily.equals(Constants.METADATA_SCANFILE_COLUMN_FAMILY)) {
        if (violations == null)
          violations = new ArrayList<Short>();
        violations.add((short) 6);
      }
      
      if (columnFamily.equals(Constants.METADATA_DATAFILE_COLUMN_FAMILY)) {
        try {
          DataFileValue dfv = new DataFileValue(columnUpdate.getValue());
          
          if (dfv.getSize() < 0 || dfv.getNumEntries() < 0) {
            if (violations == null)
              violations = new ArrayList<Short>();
            violations.add((short) 1);
          }
        } catch (NumberFormatException nfe) {
          if (violations == null)
            violations = new ArrayList<Short>();
          violations.add((short) 1);
        } catch (ArrayIndexOutOfBoundsException aiooe) {
          if (violations == null)
            violations = new ArrayList<Short>();
          violations.add((short) 1);
        }
      } else if (columnFamily.equals(Constants.METADATA_SCANFILE_COLUMN_FAMILY)) {
        
      } else {
        if (!isValidColumn(columnUpdate)) {
          if (violations == null)
            violations = new ArrayList<Short>();
          violations.add((short) 2);
        } else if (new ColumnFQ(columnUpdate).equals(Constants.METADATA_PREV_ROW_COLUMN) && columnUpdate.getValue().length > 0
            && (violations == null || !violations.contains((short) 4))) {
          KeyExtent ke = new KeyExtent(new Text(mutation.getRow()), (Text) null);
          
          Text per = KeyExtent.decodePrevEndRow(new Value(columnUpdate.getValue()));
          
          boolean prevEndRowLessThanEndRow = per == null || ke.getEndRow() == null || per.compareTo(ke.getEndRow()) < 0;
          
          if (!prevEndRowLessThanEndRow) {
            if (violations == null)
              violations = new ArrayList<Short>();
            violations.add((short) 3);
          }
        } else if (new ColumnFQ(columnUpdate).equals(Constants.METADATA_LOCK_COLUMN)) {
          if (zooCache == null) {
            zooCache = new ZooCache();
          }
          
          if (zooRoot == null) {
            zooRoot = ZooUtil.getRoot(HdfsZooInstance.getInstance());
          }
          
          boolean lockHeld = false;
          String lockId = new String(columnUpdate.getValue());
          
          try {
            lockHeld = ZooLock.isLockHeld(zooCache, new ZooUtil.LockID(zooRoot, lockId));
          } catch (Exception e) {
            log.debug("Failed to verify lock was held " + lockId + " " + e.getMessage());
          }
          
          if (!lockHeld) {
            if (violations == null)
              violations = new ArrayList<Short>();
            violations.add((short) 7);
          }
        }
        
      }
    }
    
    if (violations != null) {
      log.debug(" violating metadata mutation : " + mutation);
    }
    
    return violations;
  }
  
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
        return "Row can not be less than " + Constants.METADATA_TABLE_ID;
      case 6:
        return "Empty values are not allowed for any " + Constants.METADATA_TABLE_NAME + " column";
      case 7:
        return "Lock not held in zookeeper by writer";
    }
    return null;
  }
  
  protected void finalize() {
    if (zooCache != null)
      zooCache.clear();
  }
  
}
