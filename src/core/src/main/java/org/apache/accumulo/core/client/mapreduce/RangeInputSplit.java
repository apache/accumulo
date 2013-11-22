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
package org.apache.accumulo.core.client.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigInteger;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.mapreduce.InputFormatBase.AccumuloIterator;
import org.apache.accumulo.core.client.mapreduce.InputFormatBase.AccumuloIteratorOption;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Level;

/**
 * The Class RangeInputSplit. Encapsulates an Accumulo range for use in Map Reduce jobs.
 */
public class RangeInputSplit extends InputSplit implements Writable {
  private Range range;
  private String[] locations;
  private String table, instanceName, zooKeepers, username;
  private String rowRegex, colfamRegex, colqualRegex, valueRegex;
  private byte[] password;
  private Boolean offline, mockInstance, isolatedScan, localIterators;
  private Integer maxVersions;
  private Authorizations auths;
  private Set<Pair<Text,Text>> fetchedColumns;
  private List<AccumuloIterator> iterators;
  private List<AccumuloIteratorOption> options;
  private Level level;

  public RangeInputSplit() {
    range = new Range();
    locations = new String[0];
  }

  public RangeInputSplit(Range range, String[] locations) {
    this.range = range;
    this.locations = locations;
  }

  public Range getRange() {
    return range;
  }

  private static byte[] extractBytes(ByteSequence seq, int numBytes) {
    byte[] bytes = new byte[numBytes + 1];
    bytes[0] = 0;
    for (int i = 0; i < numBytes; i++) {
      if (i >= seq.length())
        bytes[i + 1] = 0;
      else
        bytes[i + 1] = seq.byteAt(i);
    }
    return bytes;
  }

  public static float getProgress(ByteSequence start, ByteSequence end, ByteSequence position) {
    int maxDepth = Math.min(Math.max(end.length(), start.length()), position.length());
    BigInteger startBI = new BigInteger(extractBytes(start, maxDepth));
    BigInteger endBI = new BigInteger(extractBytes(end, maxDepth));
    BigInteger positionBI = new BigInteger(extractBytes(position, maxDepth));
    return (float) (positionBI.subtract(startBI).doubleValue() / endBI.subtract(startBI).doubleValue());
  }

  public float getProgress(Key currentKey) {
    if (currentKey == null)
      return 0f;
    if (range.getStartKey() != null && range.getEndKey() != null) {
      if (range.getStartKey().compareTo(range.getEndKey(), PartialKey.ROW) != 0) {
        // just look at the row progress
        return getProgress(range.getStartKey().getRowData(), range.getEndKey().getRowData(), currentKey.getRowData());
      } else if (range.getStartKey().compareTo(range.getEndKey(), PartialKey.ROW_COLFAM) != 0) {
        // just look at the column family progress
        return getProgress(range.getStartKey().getColumnFamilyData(), range.getEndKey().getColumnFamilyData(), currentKey.getColumnFamilyData());
      } else if (range.getStartKey().compareTo(range.getEndKey(), PartialKey.ROW_COLFAM_COLQUAL) != 0) {
        // just look at the column qualifier progress
        return getProgress(range.getStartKey().getColumnQualifierData(), range.getEndKey().getColumnQualifierData(), currentKey.getColumnQualifierData());
      }
    }
    // if we can't figure it out, then claim no progress
    return 0f;
  }

  /**
   * This implementation of length is only an estimate, it does not provide exact values. Do not have your code rely on this return value.
   */
  public long getLength() throws IOException {
    Text startRow = range.isInfiniteStartKey() ? new Text(new byte[] {Byte.MIN_VALUE}) : range.getStartKey().getRow();
    Text stopRow = range.isInfiniteStopKey() ? new Text(new byte[] {Byte.MAX_VALUE}) : range.getEndKey().getRow();
    int maxCommon = Math.min(7, Math.min(startRow.getLength(), stopRow.getLength()));
    long diff = 0;

    byte[] start = startRow.getBytes();
    byte[] stop = stopRow.getBytes();
    for (int i = 0; i < maxCommon; ++i) {
      diff |= 0xff & (start[i] ^ stop[i]);
      diff <<= Byte.SIZE;
    }

    if (startRow.getLength() != stopRow.getLength())
      diff |= 0xff;

    return diff + 1;
  }

  public String[] getLocations() throws IOException {
    return locations;
  }

  public void readFields(DataInput in) throws IOException {
    range.readFields(in);
    int numLocs = in.readInt();
    locations = new String[numLocs];
    for (int i = 0; i < numLocs; ++i)
      locations[i] = in.readUTF();
    
    if (in.readBoolean()) {
      isolatedScan = in.readBoolean();
    }
    
    if (in.readBoolean()) {
      offline = in.readBoolean();
    }
    
    if (in.readBoolean()) {
      localIterators = in.readBoolean();
    }
    
    if (in.readBoolean()) {
      mockInstance = in.readBoolean();
    }
    
    if (in.readBoolean()) {
      maxVersions = in.readInt();
    }
    
    if (in.readBoolean()) {
      rowRegex = in.readUTF();
    }
    
    if (in.readBoolean()) {
      colfamRegex = in.readUTF();
    }
    
    if (in.readBoolean()) {
      colqualRegex = in.readUTF();
    }
    
    if (in.readBoolean()) {
      valueRegex = in.readUTF();
    }
    
    if (in.readBoolean()) {
      int numColumns = in.readInt();
      String[] columns = new String[numColumns];
      for (int i = 0; i < numColumns; i++) {
        columns[i] = in.readUTF();
      }
      
      fetchedColumns = InputFormatBase.deserializeFetchedColumns(columns);
    }
    
    if (in.readBoolean()) {
      auths = new Authorizations(StringUtils.split(in.readUTF()));
    }
    
    if (in.readBoolean()) {
      username = in.readUTF();
    }
    
    if (in.readBoolean()) {
      password = in.readUTF().getBytes();
    }
    
    if (in.readBoolean()) {
      instanceName = in.readUTF();
    }
    
    if (in.readBoolean()) {
      zooKeepers = in.readUTF();
    }
    
    if (in.readBoolean()) {
      level = Level.toLevel(in.readInt());
    }
  }

  public void write(DataOutput out) throws IOException {
    range.write(out);
    out.writeInt(locations.length);
    for (int i = 0; i < locations.length; ++i)
      out.writeUTF(locations[i]);
    
    out.writeBoolean(null != isolatedScan);
    if (null != isolatedScan) {
      out.writeBoolean(isolatedScan);
    }
    
    out.writeBoolean(null != offline);
    if (null != offline) {
      out.writeBoolean(offline);
    }
    
    out.writeBoolean(null != localIterators);
    if (null != localIterators) {
      out.writeBoolean(localIterators);
    }
    
    out.writeBoolean(null != mockInstance);
    if (null != mockInstance) {
      out.writeBoolean(mockInstance);
    }
    
    out.writeBoolean(null != maxVersions);
    if (null != maxVersions) {
      out.writeInt(getMaxVersions());
    }
    
    out.writeBoolean(null != rowRegex);
    if (null != rowRegex) {
      out.writeUTF(rowRegex);
    }
    
    out.writeBoolean(null != colfamRegex);
    if (null != colfamRegex) {
      out.writeUTF(colfamRegex);
    }
    
    out.writeBoolean(null != colqualRegex);
    if (null != colqualRegex) {
      out.writeUTF(colqualRegex);
    }
    
    out.writeBoolean(null != valueRegex);
    if (null != valueRegex) {
      out.writeUTF(valueRegex);
    }
    
    out.writeBoolean(null != fetchedColumns);
    if (null != fetchedColumns) {
      String[] cols = InputFormatBase.serializeColumns(fetchedColumns);
      out.writeInt(cols.length);
      for (String col : cols) {
        out.writeUTF(col);
      }
    }
    
    out.writeBoolean(null != auths);
    if (null != auths) {
      out.writeUTF(auths.serialize());
    }
    
    out.writeBoolean(null != username);
    if (null != username) {
      out.writeUTF(username);
    }
    
    out.writeBoolean(null != password);
    if (null != password) {
      out.writeUTF(new String(password));
    }
    
    out.writeBoolean(null != instanceName);
    if (null != instanceName) {
      out.writeUTF(instanceName);
    }
    
    out.writeBoolean(null != zooKeepers);
    if (null != zooKeepers) {
      out.writeUTF(zooKeepers);
    }
    
    out.writeBoolean(null != level);
    if (null != level) {
      out.writeInt(level.toInt());
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(128);
    sb.append("Range: ").append(range);
    sb.append(" Locations: ").append(locations);
    sb.append(" Table: ").append(table);
    sb.append(" InstanceName: ").append(instanceName);
    sb.append(" zooKeepers: ").append(zooKeepers);
    sb.append(" username: ").append(username);
    sb.append(" password: ").append(new String(password));
    sb.append(" Authorizations: ").append(auths);
    sb.append(" offlineScan: ").append(offline);
    sb.append(" mockInstance: ").append(mockInstance);
    sb.append(" isolatedScan: ").append(isolatedScan);
    sb.append(" localIterators: ").append(localIterators);
    sb.append(" maxVersions: ").append(maxVersions);
    sb.append(" rowRegex: ").append(rowRegex);
    sb.append(" colfamRegex: ").append(colfamRegex);
    sb.append(" colqualRegex: ").append(colqualRegex);
    sb.append(" valueRegex: ").append(valueRegex);
    sb.append(" fetchColumns: ").append(fetchedColumns);
    sb.append(" iterators: ").append(iterators);
    sb.append(" options: ").append(options);
    sb.append(" logLevel: ").append(level);
    return sb.toString();
  }

  public String getTable() {
    return table;
  }

  public void setTable(String table) {
    this.table = table;
  }
  
  public Instance getInstance() {
    if (null == instanceName) {
      return null;
    }
    
    if (isMockInstance()) {  
      return new MockInstance(getInstanceName());
    }
    
    if (null == zooKeepers) {
      return null;
    }
    
    return new ZooKeeperInstance(getInstanceName(), getZooKeepers());
  }

  public String getInstanceName() {
    return instanceName;
  }

  public void setInstanceName(String instanceName) {
    this.instanceName = instanceName;
  }

  public String getZooKeepers() {
    return zooKeepers;
  }

  public void setZooKeepers(String zooKeepers) {
    this.zooKeepers = zooKeepers;
  }

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public byte[] getPassword() {
    return password;
  }

  public void setPassword(byte[] password) {
    this.password = password;
  }

  public Boolean isOffline() {
    return offline;
  }

  public void setOffline(Boolean offline) {
    this.offline = offline;
  }

  public void setLocations(String[] locations) {
    this.locations = locations;
  }

  public String getRowRegex() {
    return rowRegex;
  }

  public void setRowRegex(String rowRegex) {
    this.rowRegex = rowRegex;
  }

  public String getColfamRegex() {
    return colfamRegex;
  }

  public void setColfamRegex(String colfamRegex) {
    this.colfamRegex = colfamRegex;
  }

  public String getColqualRegex() {
    return colqualRegex;
  }

  public void setColqualRegex(String colqualRegex) {
    this.colqualRegex = colqualRegex;
  }

  public String getValueRegex() {
    return valueRegex;
  }

  public void setValueRegex(String valueRegex) {
    this.valueRegex = valueRegex;
  }

  public Boolean isMockInstance() {
    return mockInstance;
  }

  public void setMockInstance(Boolean mockInstance) {
    this.mockInstance = mockInstance;
  }

  public Boolean isIsolatedScan() {
    return isolatedScan;
  }

  public void setIsolatedScan(Boolean isolatedScan) {
    this.isolatedScan = isolatedScan;
  }

  public Integer getMaxVersions() {
    return maxVersions;
  }

  public void setMaxVersions(Integer maxVersions) {
    this.maxVersions = maxVersions;
  }

  public Authorizations getAuths() {
    return auths;
  }

  public void setAuths(Authorizations auths) {
    this.auths = auths;
  }

  public void setRange(Range range) {
    this.range = range;
  }

  public Boolean usesLocalIterators() {
    return localIterators;
  }

  public void setUsesLocalIterators(Boolean localIterators) {
    this.localIterators = localIterators;
  }

  public Set<Pair<Text,Text>> getFetchedColumns() {
    return fetchedColumns;
  }

  public void setFetchedColumns(Set<Pair<Text,Text>> fetchedColumns) {
    this.fetchedColumns = fetchedColumns;
  }

  public List<AccumuloIterator> getIterators() {
    return iterators;
  }

  public void setIterators(List<AccumuloIterator> iterators) {
    this.iterators = iterators;
  }

  public List<AccumuloIteratorOption> getOptions() {
    return options;
  }

  public void setOptions(List<AccumuloIteratorOption> options) {
    this.options = options;
  }
  
  public Level getLogLevel() {
    return level;
  }
  
  public void setLogLevel(Level level) {
    this.level = level;
  }
}
