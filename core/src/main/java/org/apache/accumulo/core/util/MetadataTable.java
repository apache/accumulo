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
package org.apache.accumulo.core.util;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.ScannerImpl;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.hadoop.io.Text;

public class MetadataTable {
  public static class DataFileValue {
    private long size;
    private long numEntries;
    private long time = -1;
    
    public DataFileValue(long size, long numEntries, long time) {
      this.size = size;
      this.numEntries = numEntries;
      this.time = time;
    }
    
    public DataFileValue(long size, long numEntries) {
      this.size = size;
      this.numEntries = numEntries;
      this.time = -1;
    }
    
    public DataFileValue(byte[] encodedDFV) {
      String[] ba = new String(encodedDFV).split(",");
      
      size = Long.parseLong(ba[0]);
      numEntries = Long.parseLong(ba[1]);
      
      if (ba.length == 3)
        time = Long.parseLong(ba[2]);
      else
        time = -1;
    }
    
    public long getSize() {
      return size;
    }
    
    public long getNumEntries() {
      return numEntries;
    }
    
    public boolean isTimeSet() {
      return time >= 0;
    }
    
    public long getTime() {
      return time;
    }
    
    public byte[] encode() {
      if (time >= 0)
        return ("" + size + "," + numEntries + "," + time).getBytes();
      return ("" + size + "," + numEntries).getBytes();
    }
    
    public boolean equals(Object o) {
      if (o instanceof DataFileValue) {
        DataFileValue odfv = (DataFileValue) o;
        
        return size == odfv.size && numEntries == odfv.numEntries;
      }
      
      return false;
    }
    
    public int hashCode() {
      return Long.valueOf(size + numEntries).hashCode();
    }
    
    public String toString() {
      return size + " " + numEntries;
    }
    
    public void setTime(long time) {
      if (time < 0)
        throw new IllegalArgumentException();
      this.time = time;
    }
  }
  
  public static SortedMap<KeyExtent,Text> getMetadataLocationEntries(SortedMap<Key,Value> entries) {
    Key key;
    Value val;
    Text location = null;
    Value prevRow = null;
    KeyExtent ke;
    
    SortedMap<KeyExtent,Text> results = new TreeMap<KeyExtent,Text>();
    
    Text lastRowFromKey = new Text();
    
    // text obj below is meant to be reused in loop for efficiency
    Text colf = new Text();
    Text colq = new Text();
    
    for (Entry<Key,Value> entry : entries.entrySet()) {
      key = entry.getKey();
      val = entry.getValue();
      
      if (key.compareRow(lastRowFromKey) != 0) {
        prevRow = null;
        location = null;
        key.getRow(lastRowFromKey);
      }
      
      colf = key.getColumnFamily(colf);
      colq = key.getColumnQualifier(colq);
      
      // interpret the row id as a key extent
      if (colf.equals(Constants.METADATA_CURRENT_LOCATION_COLUMN_FAMILY) || colf.equals(Constants.METADATA_FUTURE_LOCATION_COLUMN_FAMILY))
        location = new Text(val.toString());
      else if (Constants.METADATA_PREV_ROW_COLUMN.equals(colf, colq))
        prevRow = new Value(val);
      
      if (location != null && prevRow != null) {
        ke = new KeyExtent(key.getRow(), prevRow);
        results.put(ke, location);
        
        location = null;
        prevRow = null;
      }
    }
    return results;
  }
  
  public static SortedMap<Text,SortedMap<ColumnFQ,Value>> getTabletEntries(Instance instance, KeyExtent ke, List<ColumnFQ> columns, AuthInfo credentials) {
    TreeMap<Key,Value> tkv = new TreeMap<Key,Value>();
    getTabletAndPrevTabletKeyValues(instance, tkv, ke, columns, credentials);
    return getTabletEntries(tkv, columns);
  }
  
  public static SortedMap<Text,SortedMap<ColumnFQ,Value>> getTabletEntries(SortedMap<Key,Value> tabletKeyValues, List<ColumnFQ> columns) {
    TreeMap<Text,SortedMap<ColumnFQ,Value>> tabletEntries = new TreeMap<Text,SortedMap<ColumnFQ,Value>>();
    
    HashSet<ColumnFQ> colSet = null;
    if (columns != null) {
      colSet = new HashSet<ColumnFQ>(columns);
    }
    
    for (Entry<Key,Value> entry : tabletKeyValues.entrySet()) {
      
      if (columns != null && !colSet.contains(new ColumnFQ(entry.getKey()))) {
        continue;
      }
      
      Text row = entry.getKey().getRow();
      
      SortedMap<ColumnFQ,Value> colVals = tabletEntries.get(row);
      if (colVals == null) {
        colVals = new TreeMap<ColumnFQ,Value>();
        tabletEntries.put(row, colVals);
      }
      
      colVals.put(new ColumnFQ(entry.getKey()), entry.getValue());
    }
    
    return tabletEntries;
  }
  
  public static void getTabletAndPrevTabletKeyValues(Instance instance, SortedMap<Key,Value> tkv, KeyExtent ke, List<ColumnFQ> columns, AuthInfo credentials) {
    Text startRow;
    Text endRow = ke.getMetadataEntry();
    
    if (ke.getPrevEndRow() == null) {
      startRow = new Text(KeyExtent.getMetadataEntry(ke.getTableId(), new Text()));
    } else {
      startRow = new Text(KeyExtent.getMetadataEntry(ke.getTableId(), ke.getPrevEndRow()));
    }
    
    Scanner scanner = new ScannerImpl(instance, credentials, Constants.METADATA_TABLE_ID, Constants.NO_AUTHS);
    
    if (columns != null) {
      for (ColumnFQ column : columns)
        ColumnFQ.fetch(scanner, column);
    }
    
    scanner.setRange(new Range(new Key(startRow), true, new Key(endRow).followingKey(PartialKey.ROW), false));
    
    tkv.clear();
    boolean successful = false;
    try {
      for (Entry<Key,Value> entry : scanner) {
        tkv.put(entry.getKey(), entry.getValue());
      }
      successful = true;
    } finally {
      if (!successful) {
        tkv.clear();
      }
    }
  }
  
  public static void getEntries(Instance instance, AuthInfo credentials, String table, boolean isTid, Map<KeyExtent,String> locations,
      SortedSet<KeyExtent> tablets) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    String tableId = isTid ? table : Tables.getNameToIdMap(instance).get(table);
    
    Scanner scanner = instance.getConnector(credentials.user, credentials.password).createScanner(Constants.METADATA_TABLE_NAME, Constants.NO_AUTHS);
    
    ColumnFQ.fetch(scanner, Constants.METADATA_PREV_ROW_COLUMN);
    scanner.fetchColumnFamily(Constants.METADATA_CURRENT_LOCATION_COLUMN_FAMILY);
    
    // position at first entry in metadata table for given table
    KeyExtent ke = new KeyExtent(new Text(tableId), new Text(), null);
    Key startKey = new Key(ke.getMetadataEntry());
    ke = new KeyExtent(new Text(tableId), null, null);
    Key endKey = new Key(ke.getMetadataEntry()).followingKey(PartialKey.ROW);
    scanner.setRange(new Range(startKey, endKey));
    
    Text colf = new Text();
    Text colq = new Text();
    
    KeyExtent currentKeyExtent = null;
    String location = null;
    Text row = null;
    // acquire this tables METADATA table entries
    boolean haveExtent = false;
    boolean haveLocation = false;
    for (Entry<Key,Value> entry : scanner) {
      if (row != null) {
        if (!row.equals(entry.getKey().getRow())) {
          currentKeyExtent = null;
          haveExtent = false;
          haveLocation = false;
          row = entry.getKey().getRow();
        }
      } else
        row = entry.getKey().getRow();
      
      colf = entry.getKey().getColumnFamily(colf);
      colq = entry.getKey().getColumnQualifier(colq);
      
      // stop scanning metadata table when another table is reached
      if (!(new KeyExtent(entry.getKey().getRow(), (Text) null)).getTableId().toString().equals(tableId))
        break;
      
      if (Constants.METADATA_PREV_ROW_COLUMN.equals(colf, colq)) {
        currentKeyExtent = new KeyExtent(entry.getKey().getRow(), entry.getValue());
        tablets.add(currentKeyExtent);
        haveExtent = true;
      } else if (colf.equals(Constants.METADATA_CURRENT_LOCATION_COLUMN_FAMILY)) {
        location = entry.getValue().toString();
        haveLocation = true;
      }
      
      if (haveExtent && haveLocation) {
        locations.put(currentKeyExtent, location);
        haveExtent = false;
        haveLocation = false;
        currentKeyExtent = null;
      }
    }
    
    validateEntries(tableId, tablets);
  }
  
  public static void validateEntries(String tableId, SortedSet<KeyExtent> tablets) throws AccumuloException {
    // sanity check of metadata table entries
    // make sure tablets has no holes, and that it starts and ends w/ null
    if (tablets.size() == 0)
      throw new AccumuloException("No entries found in metadata table for table " + tableId);
    
    if (tablets.first().getPrevEndRow() != null)
      throw new AccumuloException("Problem with metadata table, first entry for table " + tableId + "- " + tablets.first() + " - has non null prev end row");
    
    if (tablets.last().getEndRow() != null)
      throw new AccumuloException("Problem with metadata table, last entry for table " + tableId + "- " + tablets.first() + " - has non null end row");
    
    Iterator<KeyExtent> tabIter = tablets.iterator();
    Text lastEndRow = tabIter.next().getEndRow();
    while (tabIter.hasNext()) {
      KeyExtent tabke = tabIter.next();
      
      if (tabke.getPrevEndRow() == null)
        throw new AccumuloException("Problem with metadata table, it has null prev end row in middle of table " + tabke);
      
      if (!tabke.getPrevEndRow().equals(lastEndRow))
        throw new AccumuloException("Problem with metadata table, it has a hole " + tabke.getPrevEndRow() + " != " + lastEndRow);
      
      lastEndRow = tabke.getEndRow();
    }
    
    // end METADATA table sanity check
  }
  
  public static boolean isContiguousRange(KeyExtent ke, SortedSet<KeyExtent> children) {
    if (children.size() == 0)
      return false;
    
    if (children.size() == 1)
      return children.first().equals(ke);
    
    Text per = children.first().getPrevEndRow();
    Text er = children.last().getEndRow();
    
    boolean perEqual = (per == ke.getPrevEndRow() || per != null && ke.getPrevEndRow() != null && ke.getPrevEndRow().compareTo(per) == 0);
    
    boolean erEqual = (er == ke.getEndRow() || er != null && ke.getEndRow() != null && ke.getEndRow().compareTo(er) == 0);
    
    if (!perEqual || !erEqual)
      return false;
    
    Iterator<KeyExtent> iter = children.iterator();
    
    Text lastEndRow = iter.next().getEndRow();
    
    while (iter.hasNext()) {
      KeyExtent cke = iter.next();
      
      per = cke.getPrevEndRow();
      
      // something in the middle should not be null
      
      if (per == null || lastEndRow == null)
        return false;
      
      if (per.compareTo(lastEndRow) != 0)
        return false;
      
      lastEndRow = cke.getEndRow();
    }
    
    return true;
  }
  
}
