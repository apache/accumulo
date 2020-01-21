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

package org.apache.accumulo.core.client.rfile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.impl.BaseIteratorEnvironment;
import org.apache.accumulo.core.client.impl.ScannerOptions;
import org.apache.accumulo.core.client.rfile.RFileScannerBuilder.InputArgs;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.blockfile.cache.BlockCache;
import org.apache.accumulo.core.file.blockfile.cache.CacheEntry;
import org.apache.accumulo.core.file.blockfile.cache.LruBlockCache;
import org.apache.accumulo.core.file.blockfile.cache.LruBlockCache.Options;
import org.apache.accumulo.core.file.blockfile.impl.CachableBlockFile;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.file.rfile.RFile.Reader;
import org.apache.accumulo.core.iterators.IteratorAdapter;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.system.MultiIterator;
import org.apache.accumulo.core.sample.impl.SamplerConfigurationImpl;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.LocalityGroupUtil;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.Text;

import com.google.common.base.Preconditions;

class RFileScanner extends ScannerOptions implements Scanner {

  private static final byte[] EMPTY_BYTES = new byte[0];
  private static final Range EMPTY_RANGE = new Range();

  private Range range;
  private BlockCache dataCache = null;
  private BlockCache indexCache = null;
  private Opts opts;
  private int batchSize = 1000;
  private long readaheadThreshold = 3;

  private static final long CACHE_BLOCK_SIZE = AccumuloConfiguration.getDefaultConfiguration()
      .getMemoryInBytes(Property.TSERV_DEFAULT_BLOCKSIZE);

  private static final EnumSet<Options> CACHE_OPTS;

  static {
    EnumSet<Options> cacheOpts = EnumSet.allOf(Options.class);
    cacheOpts.remove(Options.ENABLE_LOCKS);
    CACHE_OPTS = cacheOpts;
  }

  static class Opts {
    InputArgs in;
    Authorizations auths = Authorizations.EMPTY;
    long dataCacheSize;
    long indexCacheSize;
    boolean useSystemIterators = true;
    public HashMap<String,String> tableConfig;
    Range bounds;
  }

  // This cache exist as a hack to avoid leaking decompressors. When the RFile code is not given a
  // cache it reads blocks directly from the decompressor. However if a user does not read all data
  // for a scan this can leave a BCFile block open and a decompressor allocated.
  //
  // By providing a cache to the RFile code it forces each block to be read into memory. When a
  // block is accessed the entire thing is read into memory immediately allocating and deallocating
  // a decompressor. If the user does not read all data, no decompressors are left allocated.
  private static class NoopCache implements BlockCache {
    @Override
    public CacheEntry cacheBlock(String blockName, byte[] buf, boolean inMemory) {
      return null;
    }

    @Override
    public CacheEntry cacheBlock(String blockName, byte[] buf) {
      return null;
    }

    @Override
    public CacheEntry getBlock(String blockName) {
      return null;
    }

    @Override
    public CacheEntry getBlockNoStats(String blockName) {
      return null;
    }

    @Override
    public long getMaxSize() {
      return Integer.MAX_VALUE;
    }

    @Override
    public Lock getLoadLock(String blockName) {
      return null;
    }
  }

  RFileScanner(Opts opts) {
    if (!opts.auths.equals(Authorizations.EMPTY) && !opts.useSystemIterators) {
      throw new IllegalArgumentException(
          "Set authorizations and specified not to use system iterators");
    }

    this.opts = opts;
    if (opts.indexCacheSize > 0) {
      this.indexCache = new LruBlockCache(opts.indexCacheSize, CACHE_BLOCK_SIZE, CACHE_OPTS);
    } else {
      this.indexCache = new NoopCache();
    }

    if (opts.dataCacheSize > 0) {
      this.dataCache = new LruBlockCache(opts.dataCacheSize, CACHE_BLOCK_SIZE, CACHE_OPTS);
    } else {
      this.dataCache = new NoopCache();
    }
  }

  @Override
  public synchronized void fetchColumnFamily(Text col) {
    Preconditions.checkArgument(opts.useSystemIterators,
        "Can only fetch columns when using system iterators");
    super.fetchColumnFamily(col);
  }

  @Override
  public synchronized void fetchColumn(Text colFam, Text colQual) {
    Preconditions.checkArgument(opts.useSystemIterators,
        "Can only fetch columns when using system iterators");
    super.fetchColumn(colFam, colQual);
  }

  @Override
  public void fetchColumn(IteratorSetting.Column column) {
    Preconditions.checkArgument(opts.useSystemIterators,
        "Can only fetch columns when using system iterators");
    super.fetchColumn(column);
  }

  @Override
  public void setClassLoaderContext(String classLoaderContext) {
    throw new UnsupportedOperationException();
  }

  @Deprecated
  @Override
  public void setTimeOut(int timeOut) {
    if (timeOut == Integer.MAX_VALUE)
      setTimeout(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    else
      setTimeout(timeOut, TimeUnit.SECONDS);
  }

  @Deprecated
  @Override
  public int getTimeOut() {
    long timeout = getTimeout(TimeUnit.SECONDS);
    if (timeout >= Integer.MAX_VALUE)
      return Integer.MAX_VALUE;
    return (int) timeout;
  }

  @Override
  public void setRange(Range range) {
    this.range = range;
  }

  @Override
  public Range getRange() {
    return range;
  }

  @Override
  public void setBatchSize(int size) {
    this.batchSize = size;
  }

  @Override
  public int getBatchSize() {
    return batchSize;
  }

  @Override
  public void enableIsolation() {}

  @Override
  public void disableIsolation() {}

  @Override
  public synchronized void setReadaheadThreshold(long batches) {
    Preconditions.checkArgument(batches > 0);
    readaheadThreshold = batches;
  }

  @Override
  public synchronized long getReadaheadThreshold() {
    return readaheadThreshold;
  }

  @Override
  public Authorizations getAuthorizations() {
    return opts.auths;
  }

  @Override
  public void addScanIterator(IteratorSetting cfg) {
    super.addScanIterator(cfg);
  }

  @Override
  public void removeScanIterator(String iteratorName) {
    super.removeScanIterator(iteratorName);
  }

  @Override
  public void updateScanIteratorOption(String iteratorName, String key, String value) {
    super.updateScanIteratorOption(iteratorName, key, value);
  }

  private class IterEnv extends BaseIteratorEnvironment {
    @Override
    public IteratorScope getIteratorScope() {
      return IteratorScope.scan;
    }

    @Override
    public boolean isFullMajorCompaction() {
      return false;
    }

    @Override
    public Authorizations getAuthorizations() {
      return opts.auths;
    }

    @Override
    public boolean isSamplingEnabled() {
      return RFileScanner.this.getSamplerConfiguration() != null;
    }

    @Override
    public SamplerConfiguration getSamplerConfiguration() {
      return RFileScanner.this.getSamplerConfiguration();
    }
  }

  @Override
  public Iterator<Entry<Key,Value>> iterator() {
    try {
      RFileSource[] sources = opts.in.getSources();
      List<SortedKeyValueIterator<Key,Value>> readers = new ArrayList<>(sources.length);
      for (int i = 0; i < sources.length; i++) {
        FSDataInputStream inputStream = (FSDataInputStream) sources[i].getInputStream();

        readers.add(new RFile.Reader(new CachableBlockFile.Reader("source-" + i, inputStream,
            sources[i].getLength(), opts.in.getConf(), dataCache, indexCache,
            AccumuloConfiguration.getDefaultConfiguration())));
      }

      if (getSamplerConfiguration() != null) {
        for (int i = 0; i < readers.size(); i++) {
          readers.set(i, ((Reader) readers.get(i))
              .getSample(new SamplerConfigurationImpl(getSamplerConfiguration())));
        }
      }

      SortedKeyValueIterator<Key,Value> iterator;
      if (opts.bounds != null) {
        iterator = new MultiIterator(readers, opts.bounds);
      } else {
        iterator = new MultiIterator(readers, false);
      }

      Set<ByteSequence> families = Collections.emptySet();

      if (opts.useSystemIterators) {
        SortedSet<Column> cols = this.getFetchedColumns();
        families = LocalityGroupUtil.families(cols);
        iterator =
            IteratorUtil.setupSystemScanIterators(iterator, cols, getAuthorizations(), EMPTY_BYTES);
      }

      try {
        if (opts.tableConfig != null && opts.tableConfig.size() > 0) {
          ConfigurationCopy conf = new ConfigurationCopy(opts.tableConfig);
          iterator = IteratorUtil.loadIterators(IteratorScope.scan, iterator, null, conf,
              serverSideIteratorList, serverSideIteratorOptions, new IterEnv());
        } else {
          iterator = IteratorUtil.loadIterators(iterator, serverSideIteratorList,
              serverSideIteratorOptions, new IterEnv(), false, null);
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      iterator.seek(getRange() == null ? EMPTY_RANGE : getRange(), families,
          families.size() == 0 ? false : true);
      return new IteratorAdapter(iterator);

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
    if (dataCache instanceof LruBlockCache) {
      ((LruBlockCache) dataCache).shutdown();
    }

    if (indexCache instanceof LruBlockCache) {
      ((LruBlockCache) indexCache).shutdown();
    }

    try {
      for (RFileSource source : opts.in.getSources()) {
        source.getInputStream().close();
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
