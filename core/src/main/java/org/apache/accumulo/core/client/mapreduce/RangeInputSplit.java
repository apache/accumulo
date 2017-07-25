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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.mapreduce.impl.SplitUtils;
import org.apache.accumulo.core.client.mapreduce.lib.impl.ConfiguratorBase.TokenSource;
import org.apache.accumulo.core.client.mapreduce.lib.impl.InputConfigurator;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken.AuthenticationTokenSerializer;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.sample.impl.SamplerConfigurationImpl;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.DeprecationUtil;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.log4j.Level;

/**
 * The Class RangeInputSplit. Encapsulates an Accumulo range for use in Map Reduce jobs.
 */
public class RangeInputSplit extends InputSplit implements Writable {
  private Range range;
  private String[] locations;
  private String tableId, tableName, instanceName, zooKeepers, principal;
  private TokenSource tokenSource;
  private String tokenFile;
  private AuthenticationToken token;
  private Boolean offline, mockInstance, isolatedScan, localIterators;
  private Authorizations auths;
  private Set<Pair<Text,Text>> fetchedColumns;
  private List<IteratorSetting> iterators;
  private SamplerConfiguration samplerConfig;
  private Level level;

  public RangeInputSplit() {
    range = new Range();
    locations = new String[0];
    tableName = "";
    tableId = "";
  }

  public RangeInputSplit(RangeInputSplit split) throws IOException {
    this.range = split.getRange();
    this.setLocations(split.getLocations());
    this.setTableName(split.getTableName());
    this.setTableId(split.getTableId());
  }

  protected RangeInputSplit(String table, String tableId, Range range, String[] locations) {
    this.range = range;
    setLocations(locations);
    this.tableName = table;
    this.tableId = tableId;
  }

  public Range getRange() {
    return range;
  }

  public static float getProgress(ByteSequence start, ByteSequence end, ByteSequence position) {
    return SplitUtils.getProgress(start, end, position);
  }

  public float getProgress(Key currentKey) {
    if (currentKey == null)
      return 0f;
    if (range.contains(currentKey)) {
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
    }
    // if we can't figure it out, then claim no progress
    return 0f;
  }

  /**
   * This implementation of length is only an estimate, it does not provide exact values. Do not have your code rely on this return value.
   */
  @Override
  public long getLength() throws IOException {
    return SplitUtils.getRangeLength(range);
  }

  @Override
  public String[] getLocations() throws IOException {
    return Arrays.copyOf(locations, locations.length);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    range.readFields(in);
    tableName = in.readUTF();
    tableId = in.readUTF();
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
      int numColumns = in.readInt();
      List<String> columns = new ArrayList<>(numColumns);
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
          byte[] tokenBytes = Base64.getDecoder().decode(in.readUTF());

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
      iterators = new ArrayList<>(numIterators);
      for (int i = 0; i < numIterators; i++) {
        iterators.add(new IteratorSetting(in));
      }
    }

    if (in.readBoolean()) {
      level = Level.toLevel(in.readInt());
    }

    if (in.readBoolean()) {
      samplerConfig = new SamplerConfigurationImpl(in).toSamplerConfiguration();
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    range.write(out);
    out.writeUTF(tableName);
    out.writeUTF(tableId);
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
        out.writeUTF(token.getClass().getName());
        out.writeUTF(Base64.getEncoder().encodeToString(AuthenticationTokenSerializer.serialize(token)));
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

    out.writeBoolean(null != samplerConfig);
    if (null != samplerConfig) {
      new SamplerConfigurationImpl(samplerConfig).write(out);
    }
  }

  /**
   * Use {@link #getTableName}
   *
   * @deprecated since 1.6.1, use getTableName() instead.
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
   *
   * @deprecated since 1.6.1, use setTableName() instead.
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
   * @deprecated since 1.7.0, use getInstance(ClientConfiguration) instead.
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
      return DeprecationUtil.makeMockInstance(getInstanceName());
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

  public Boolean isOffline() {
    return offline;
  }

  public void setOffline(Boolean offline) {
    this.offline = offline;
  }

  public void setLocations(String[] locations) {
    this.locations = Arrays.copyOf(locations, locations.length);
  }

  /**
   * @deprecated since 1.8.0; use MiniAccumuloCluster or a standard mock framework
   */
  @Deprecated
  public Boolean isMockInstance() {
    return mockInstance;
  }

  /**
   * @deprecated since 1.8.0; use MiniAccumuloCluster or a standard mock framework
   */
  @Deprecated
  public void setMockInstance(Boolean mockInstance) {
    this.mockInstance = mockInstance;
  }

  public Boolean isIsolatedScan() {
    return isolatedScan;
  }

  public void setIsolatedScan(Boolean isolatedScan) {
    this.isolatedScan = isolatedScan;
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

  public void setFetchedColumns(Collection<Pair<Text,Text>> fetchedColumns) {
    this.fetchedColumns = new HashSet<>();
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

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(256);
    sb.append("Range: ").append(range);
    sb.append(" Locations: ").append(Arrays.asList(locations));
    sb.append(" Table: ").append(tableName);
    sb.append(" TableID: ").append(tableId);
    sb.append(" InstanceName: ").append(instanceName);
    sb.append(" zooKeepers: ").append(zooKeepers);
    sb.append(" principal: ").append(principal);
    sb.append(" tokenSource: ").append(tokenSource);
    sb.append(" authenticationToken: ").append(token);
    sb.append(" authenticationTokenFile: ").append(tokenFile);
    sb.append(" Authorizations: ").append(auths);
    sb.append(" offlineScan: ").append(offline);
    sb.append(" mockInstance: ").append(mockInstance);
    sb.append(" isolatedScan: ").append(isolatedScan);
    sb.append(" localIterators: ").append(localIterators);
    sb.append(" fetchColumns: ").append(fetchedColumns);
    sb.append(" iterators: ").append(iterators);
    sb.append(" logLevel: ").append(level);
    sb.append(" samplerConfig: ").append(samplerConfig);
    return sb.toString();
  }

  public void setSamplerConfiguration(SamplerConfiguration samplerConfiguration) {
    this.samplerConfig = samplerConfiguration;
  }

  public SamplerConfiguration getSamplerConfiguration() {
    return samplerConfig;
  }
}
