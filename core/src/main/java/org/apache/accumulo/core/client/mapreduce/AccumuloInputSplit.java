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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.mapreduce.lib.impl.ConfiguratorBase.TokenSource;
import org.apache.accumulo.core.client.mapreduce.lib.impl.InputConfigurator;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken.AuthenticationTokenSerializer;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.Base64;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.log4j.Level;

/**
 * Abstracts over configurations common to all InputSplits. Specifically it leaves out methods
 * related to number of ranges and locations per InputSplit as those vary by implementation.
 *
 * @see RangeInputSplit
 * @see BatchInputSplit
 */
public abstract class AccumuloInputSplit extends InputSplit implements Writable {
  protected String[] locations;
  protected String tableId, tableName, instanceName, zooKeepers, principal;
  protected TokenSource tokenSource;
  protected String tokenFile;
  protected AuthenticationToken token;
  protected Boolean mockInstance;
  protected Authorizations auths;
  protected Set<Pair<Text,Text>> fetchedColumns;
  protected List<IteratorSetting> iterators;
  protected Level level;

  public abstract float getProgress(Key currentKey);

  public AccumuloInputSplit() {
    locations = new String[0];
    tableName = "";
    tableId = "";
  }

  public AccumuloInputSplit(AccumuloInputSplit split) throws IOException {
    this.setLocations(split.getLocations());
    this.setTableName(split.getTableName());
    this.setTableId(split.getTableId());
  }

  protected AccumuloInputSplit(String table, String tableId, String[] locations) {
    setLocations(locations);
    this.tableName = table;
    this.tableId = tableId;
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

  public long getRangeLength(Range range) throws IOException {
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

  @Override
  public String[] getLocations() throws IOException {
    return Arrays.copyOf(locations, locations.length);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    tableName = in.readUTF();
    tableId = in.readUTF();
    int numLocs = in.readInt();
    locations = new String[numLocs];
    for (int i = 0; i < numLocs; ++i)
      locations[i] = in.readUTF();

    if (in.readBoolean()) {
      mockInstance = in.readBoolean();
    }

    if (in.readBoolean()) {
      int numColumns = in.readInt();
      List<String> columns = new ArrayList<String>(numColumns);
      for (int i = 0; i < numColumns; i++) {
        columns.add(in.readUTF());
      }

      fetchedColumns = InputConfigurator.deserializeFetchedColumns(columns);
    }

    if (in.readBoolean()) {
      String strAuths = in.readUTF();
      auths = new Authorizations(strAuths.getBytes(UTF_8));
    }

    if (in.readBoolean()) {
      principal = in.readUTF();
    }

    if (in.readBoolean()) {
      int ordinal = in.readInt();
      this.tokenSource = TokenSource.values()[ordinal];

      switch (this.tokenSource) {
        case INLINE:
          String tokenClass = in.readUTF();
          byte[] base64TokenBytes = in.readUTF().getBytes(UTF_8);
          byte[] tokenBytes = Base64.decodeBase64(base64TokenBytes);

          this.token = AuthenticationTokenSerializer.deserialize(tokenClass, tokenBytes);
          break;

        case FILE:
          this.tokenFile = in.readUTF();

          break;
        default:
          throw new IOException("Cannot parse unknown TokenSource ordinal");
      }
    }

    if (in.readBoolean()) {
      instanceName = in.readUTF();
    }

    if (in.readBoolean()) {
      zooKeepers = in.readUTF();
    }

    if (in.readBoolean()) {
      int numIterators = in.readInt();
      iterators = new ArrayList<IteratorSetting>(numIterators);
      for (int i = 0; i < numIterators; i++) {
        iterators.add(new IteratorSetting(in));
      }
    }

    if (in.readBoolean()) {
      level = Level.toLevel(in.readInt());
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(tableName);
    out.writeUTF(tableId);
    out.writeInt(locations.length);
    for (int i = 0; i < locations.length; ++i)
      out.writeUTF(locations[i]);

    out.writeBoolean(null != mockInstance);
    if (null != mockInstance) {
      out.writeBoolean(mockInstance);
    }

    out.writeBoolean(null != fetchedColumns);
    if (null != fetchedColumns) {
      String[] cols = InputConfigurator.serializeColumns(fetchedColumns);
      out.writeInt(cols.length);
      for (String col : cols) {
        out.writeUTF(col);
      }
    }

    out.writeBoolean(null != auths);
    if (null != auths) {
      out.writeUTF(auths.serialize());
    }

    out.writeBoolean(null != principal);
    if (null != principal) {
      out.writeUTF(principal);
    }

    out.writeBoolean(null != tokenSource);
    if (null != tokenSource) {
      out.writeInt(tokenSource.ordinal());

      if (null != token && null != tokenFile) {
        throw new IOException("Cannot use both inline AuthenticationToken and file-based AuthenticationToken");
      } else if (null != token) {
        out.writeUTF(token.getClass().getCanonicalName());
        out.writeUTF(Base64.encodeBase64String(AuthenticationTokenSerializer.serialize(token)));
      } else {
        out.writeUTF(tokenFile);
      }
    }

    out.writeBoolean(null != instanceName);
    if (null != instanceName) {
      out.writeUTF(instanceName);
    }

    out.writeBoolean(null != zooKeepers);
    if (null != zooKeepers) {
      out.writeUTF(zooKeepers);
    }

    out.writeBoolean(null != iterators);
    if (null != iterators) {
      out.writeInt(iterators.size());
      for (IteratorSetting iterator : iterators) {
        iterator.write(out);
      }
    }

    out.writeBoolean(null != level);
    if (null != level) {
      out.writeInt(level.toInt());
    }
  }

  /**
   * Use {@link #getTableName}
   */
  @Deprecated
  public String getTable() {
    return getTableName();
  }

  public String getTableName() {
    return tableName;
  }

  /**
   * Use {@link #setTableName}
   */
  @Deprecated
  public void setTable(String table) {
    setTableName(table);
  }

  public void setTableName(String table) {
    this.tableName = table;
  }

  public void setTableId(String tableId) {
    this.tableId = tableId;
  }

  public String getTableId() {
    return tableId;
  }

  /**
   * @see #getInstance(ClientConfiguration)
   */
  @Deprecated
  public Instance getInstance() {
    return getInstance(ClientConfiguration.loadDefault());
  }

  public Instance getInstance(ClientConfiguration base) {
    if (null == instanceName) {
      return null;
    }

    if (isMockInstance()) {
      return new MockInstance(getInstanceName());
    }

    if (null == zooKeepers) {
      return null;
    }

    return new ZooKeeperInstance(base.withInstance(getInstanceName()).withZkHosts(getZooKeepers()));
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

  public String getPrincipal() {
    return principal;
  }

  public void setPrincipal(String principal) {
    this.principal = principal;
  }

  public AuthenticationToken getToken() {
    return token;
  }

  public void setToken(AuthenticationToken token) {
    this.tokenSource = TokenSource.INLINE;
    this.token = token;
  }

  public void setToken(String tokenFile) {
    this.tokenSource = TokenSource.FILE;
    this.tokenFile = tokenFile;
  }

  public void setLocations(String[] locations) {
    this.locations = Arrays.copyOf(locations, locations.length);
  }

  public Boolean isMockInstance() {
    return mockInstance;
  }

  public void setMockInstance(Boolean mockInstance) {
    this.mockInstance = mockInstance;
  }

  public Authorizations getAuths() {
    return auths;
  }

  public void setAuths(Authorizations auths) {
    this.auths = auths;
  }


  public Set<Pair<Text,Text>> getFetchedColumns() {
    return fetchedColumns;
  }

  public void setFetchedColumns(Collection<Pair<Text,Text>> fetchedColumns) {
    this.fetchedColumns = new HashSet<Pair<Text,Text>>();
    for (Pair<Text,Text> columns : fetchedColumns) {
      this.fetchedColumns.add(columns);
    }
  }

  public void setFetchedColumns(Set<Pair<Text,Text>> fetchedColumns) {
    this.fetchedColumns = fetchedColumns;
  }

  public List<IteratorSetting> getIterators() {
    return iterators;
  }

  public void setIterators(List<IteratorSetting> iterators) {
    this.iterators = iterators;
  }

  public Level getLogLevel() {
    return level;
  }

  public void setLogLevel(Level level) {
    this.level = level;
  }
}
