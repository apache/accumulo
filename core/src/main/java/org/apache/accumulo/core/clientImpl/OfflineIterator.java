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
package org.apache.accumulo.core.clientImpl;

import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.SampleNotPresentException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.crypto.CryptoServiceFactory;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyValue;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.system.MultiIterator;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.sample.impl.SamplerConfigurationImpl;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.LocalityGroupUtil;
import org.apache.accumulo.core.volume.VolumeConfiguration;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;

class OfflineIterator implements Iterator<Entry<Key,Value>> {

  static class OfflineIteratorEnvironment implements IteratorEnvironment {

    private final Authorizations authorizations;
    private AccumuloConfiguration conf;
    private boolean useSample;
    private SamplerConfiguration sampleConf;

    public OfflineIteratorEnvironment(Authorizations auths, AccumuloConfiguration acuTableConf,
        boolean useSample, SamplerConfiguration samplerConf) {
      this.authorizations = auths;
      this.conf = acuTableConf;
      this.useSample = useSample;
      this.sampleConf = samplerConf;
    }

    @Override
    public SortedKeyValueIterator<Key,Value> reserveMapFileReader(String mapFileName)
        throws IOException {
      throw new NotImplementedException();
    }

    @Override
    public AccumuloConfiguration getConfig() {
      return conf;
    }

    @Override
    public IteratorScope getIteratorScope() {
      return IteratorScope.scan;
    }

    @Override
    public boolean isFullMajorCompaction() {
      return false;
    }

    @Override
    public boolean isUserCompaction() {
      return false;
    }

    private ArrayList<SortedKeyValueIterator<Key,Value>> topLevelIterators = new ArrayList<>();

    @Override
    public void registerSideChannel(SortedKeyValueIterator<Key,Value> iter) {
      topLevelIterators.add(iter);
    }

    @Override
    public Authorizations getAuthorizations() {
      return authorizations;
    }

    SortedKeyValueIterator<Key,Value> getTopLevelIterator(SortedKeyValueIterator<Key,Value> iter) {
      if (topLevelIterators.isEmpty())
        return iter;
      ArrayList<SortedKeyValueIterator<Key,Value>> allIters = new ArrayList<>(topLevelIterators);
      allIters.add(iter);
      return new MultiIterator(allIters, false);
    }

    @Override
    public boolean isSamplingEnabled() {
      return useSample;
    }

    @Override
    public SamplerConfiguration getSamplerConfiguration() {
      return sampleConf;
    }

    @Override
    public IteratorEnvironment cloneWithSamplingEnabled() {
      if (sampleConf == null)
        throw new SampleNotPresentException();
      return new OfflineIteratorEnvironment(authorizations, conf, true, sampleConf);
    }
  }

  private SortedKeyValueIterator<Key,Value> iter;
  private Range range;
  private KeyExtent currentExtent;
  private AccumuloClient client;
  private Table.ID tableId;
  private Authorizations authorizations;
  private ClientContext context;
  private ScannerOptions options;
  private ArrayList<SortedKeyValueIterator<Key,Value>> readers;
  private AccumuloConfiguration config;

  public OfflineIterator(ScannerOptions options, ClientContext context,
      Authorizations authorizations, Text table, Range range) {
    this.options = new ScannerOptions(options);
    this.context = context;
    this.range = range;

    if (this.options.fetchedColumns.size() > 0) {
      this.range = range.bound(this.options.fetchedColumns.first(),
          this.options.fetchedColumns.last());
    }

    this.tableId = Table.ID.of(table.toString());
    this.authorizations = authorizations;
    this.readers = new ArrayList<>();

    try {
      client = context.getClient();
      config = new ConfigurationCopy(client.instanceOperations().getSiteConfiguration());
      nextTablet();

      while (iter != null && !iter.hasTop())
        nextTablet();

    } catch (Exception e) {
      if (e instanceof RuntimeException)
        throw (RuntimeException) e;
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean hasNext() {
    return iter != null && iter.hasTop();
  }

  @Override
  public Entry<Key,Value> next() {
    try {
      byte[] v = iter.getTopValue().get();
      // copy just like tablet server does, do this before calling next
      KeyValue ret = new KeyValue(new Key(iter.getTopKey()), Arrays.copyOf(v, v.length));

      iter.next();

      while (iter != null && !iter.hasTop())
        nextTablet();

      return ret;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void nextTablet()
      throws TableNotFoundException, AccumuloException, IOException, AccumuloSecurityException {

    Range nextRange = null;

    if (currentExtent == null) {
      Text startRow;

      if (range.getStartKey() != null)
        startRow = range.getStartKey().getRow();
      else
        startRow = new Text();

      nextRange = new Range(TabletsSection.getRow(tableId, startRow), true, null, false);
    } else {

      if (currentExtent.getEndRow() == null) {
        iter = null;
        return;
      }

      if (range.afterEndKey(new Key(currentExtent.getEndRow()).followingKey(PartialKey.ROW))) {
        iter = null;
        return;
      }

      nextRange = new Range(currentExtent.getMetadataEntry(), false, null, false);
    }

    TabletMetadata tablet = getTabletFiles(nextRange);

    while (tablet.getLocation() != null) {
      if (Tables.getTableState(context, tableId) != TableState.OFFLINE) {
        Tables.clearCache(context);
        if (Tables.getTableState(context, tableId) != TableState.OFFLINE) {
          throw new AccumuloException("Table is online " + tableId
              + " cannot scan tablet in offline mode " + tablet.getExtent());
        }
      }

      sleepUninterruptibly(250, TimeUnit.MILLISECONDS);

      tablet = getTabletFiles(nextRange);
    }

    if (!tablet.getExtent().getTableId().equals(tableId)) {
      throw new AccumuloException(
          " did not find tablets for table " + tableId + " " + tablet.getExtent());
    }

    if (currentExtent != null && !tablet.getExtent().isPreviousExtent(currentExtent))
      throw new AccumuloException(
          " " + currentExtent + " is not previous extent " + tablet.getExtent());

    // Old property is only used to resolve relative paths into absolute paths. For systems upgraded
    // with relative paths, it's assumed that correct instance.dfs.{uri,dir} is still correct in the
    // configuration
    @SuppressWarnings("deprecation")
    String tablesDir = config.get(Property.INSTANCE_DFS_DIR) + Constants.HDFS_TABLES_DIR;

    List<String> absFiles = new ArrayList<>();
    for (String relPath : tablet.getFiles()) {
      if (relPath.contains(":")) {
        absFiles.add(relPath);
      } else {
        // handle old-style relative paths
        if (relPath.startsWith("..")) {
          absFiles.add(tablesDir + relPath.substring(2));
        } else {
          absFiles.add(tablesDir + "/" + tableId + relPath);
        }
      }
    }

    iter = createIterator(tablet.getExtent(), absFiles);
    iter.seek(range, LocalityGroupUtil.families(options.fetchedColumns),
        options.fetchedColumns.size() != 0);
    currentExtent = tablet.getExtent();

  }

  private TabletMetadata getTabletFiles(Range nextRange)
      throws TableNotFoundException, AccumuloException, AccumuloSecurityException {

    try (TabletsMetadata tablets = TabletsMetadata.builder().scanMetadataTable()
        .overRange(nextRange).fetchFiles().fetchLocation().fetchPrev().build(client)) {
      return tablets.iterator().next();
    }
  }

  private SortedKeyValueIterator<Key,Value> createIterator(KeyExtent extent, List<String> absFiles)
      throws TableNotFoundException, AccumuloException, IOException {

    // TODO share code w/ tablet - ACCUMULO-1303

    // possible race condition here, if table is renamed
    String tableName = Tables.getTableName(context, tableId);
    AccumuloConfiguration acuTableConf = new ConfigurationCopy(
        client.tableOperations().getProperties(tableName));

    Configuration conf = CachedConfiguration.getInstance();

    for (SortedKeyValueIterator<Key,Value> reader : readers) {
      ((FileSKVIterator) reader).close();
    }

    readers.clear();

    SamplerConfiguration scannerSamplerConfig = options.getSamplerConfiguration();
    SamplerConfigurationImpl scannerSamplerConfigImpl = scannerSamplerConfig == null ? null
        : new SamplerConfigurationImpl(scannerSamplerConfig);
    SamplerConfigurationImpl samplerConfImpl = SamplerConfigurationImpl
        .newSamplerConfig(acuTableConf);

    if (scannerSamplerConfigImpl != null
        && ((samplerConfImpl != null && !scannerSamplerConfigImpl.equals(samplerConfImpl))
            || samplerConfImpl == null)) {
      throw new SampleNotPresentException();
    }

    // TODO need to close files - ACCUMULO-1303
    for (String file : absFiles) {
      FileSystem fs = VolumeConfiguration.getVolume(file, conf, config).getFileSystem();
      FileSKVIterator reader = FileOperations.getInstance().newReaderBuilder()
          .forFile(file, fs, conf, CryptoServiceFactory.newDefaultInstance())
          .withTableConfiguration(acuTableConf).build();
      if (scannerSamplerConfigImpl != null) {
        reader = reader.getSample(scannerSamplerConfigImpl);
        if (reader == null)
          throw new SampleNotPresentException();
      }
      readers.add(reader);
    }

    MultiIterator multiIter = new MultiIterator(readers, extent);

    OfflineIteratorEnvironment iterEnv = new OfflineIteratorEnvironment(authorizations,
        acuTableConf, false,
        samplerConfImpl == null ? null : samplerConfImpl.toSamplerConfiguration());

    byte[] defaultSecurityLabel;
    ColumnVisibility cv = new ColumnVisibility(
        acuTableConf.get(Property.TABLE_DEFAULT_SCANTIME_VISIBILITY));
    defaultSecurityLabel = cv.getExpression();

    SortedKeyValueIterator<Key,Value> visFilter = IteratorUtil.setupSystemScanIterators(multiIter,
        new HashSet<>(options.fetchedColumns), authorizations, defaultSecurityLabel, acuTableConf);

    return iterEnv.getTopLevelIterator(
        IteratorUtil.loadIterators(IteratorScope.scan, visFilter, extent, acuTableConf,
            options.serverSideIteratorList, options.serverSideIteratorOptions, iterEnv, false));
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

}
