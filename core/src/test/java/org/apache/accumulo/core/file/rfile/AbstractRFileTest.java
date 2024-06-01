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
package org.apache.accumulo.core.file.rfile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.accumulo.core.client.sample.Sampler;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.crypto.CryptoFactoryLoader;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.blockfile.cache.impl.BlockCacheConfiguration;
import org.apache.accumulo.core.file.blockfile.cache.impl.BlockCacheManagerFactory;
import org.apache.accumulo.core.file.blockfile.cache.lru.LruBlockCache;
import org.apache.accumulo.core.file.blockfile.cache.lru.LruBlockCacheManager;
import org.apache.accumulo.core.file.blockfile.impl.BasicCacheProvider;
import org.apache.accumulo.core.file.blockfile.impl.CachableBlockFile.CachableBuilder;
import org.apache.accumulo.core.file.rfile.RFile.FencedReader;
import org.apache.accumulo.core.file.rfile.RFile.Reader;
import org.apache.accumulo.core.file.rfile.RFileTest.SeekableByteArrayInputStream;
import org.apache.accumulo.core.file.rfile.bcfile.BCFile;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.system.ColumnFamilySkippingIterator;
import org.apache.accumulo.core.sample.impl.SamplerConfigurationImpl;
import org.apache.accumulo.core.sample.impl.SamplerFactory;
import org.apache.accumulo.core.spi.cache.BlockCacheManager;
import org.apache.accumulo.core.spi.cache.CacheType;
import org.apache.accumulo.core.spi.crypto.CryptoEnvironment;
import org.apache.accumulo.core.spi.crypto.CryptoService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;

public abstract class AbstractRFileTest {

  protected static final SecureRandom random = new SecureRandom();
  protected static final Collection<ByteSequence> EMPTY_COL_FAMS = List.of();

  protected AccumuloConfiguration conf = null;

  public static class TestRFile {

    protected Configuration conf = new Configuration();
    public RFile.Writer writer;
    protected ByteArrayOutputStream baos;
    protected FSDataOutputStream dos;
    protected SeekableByteArrayInputStream bais;
    protected FSDataInputStream in;
    protected AccumuloConfiguration accumuloConfiguration;
    public Reader reader;
    public SortedKeyValueIterator<Key,Value> iter;
    private BlockCacheManager manager;

    public TestRFile(AccumuloConfiguration accumuloConfiguration) {
      this.accumuloConfiguration = accumuloConfiguration;
      if (this.accumuloConfiguration == null) {
        this.accumuloConfiguration = DefaultConfiguration.getInstance();
      }
    }

    public void openWriter(boolean startDLG) throws IOException {
      openWriter(startDLG, 1000);
    }

    public void openWriter(boolean startDLG, int blockSize) throws IOException {
      openWriter(startDLG, blockSize, 1000);
    }

    public void openWriter(boolean startDLG, int blockSize, int indexBlockSize) throws IOException {
      baos = new ByteArrayOutputStream();
      dos = new FSDataOutputStream(baos, new FileSystem.Statistics("a"));
      CryptoService cs = CryptoFactoryLoader.getServiceForClient(CryptoEnvironment.Scope.TABLE,
          accumuloConfiguration.getAllCryptoProperties());

      BCFile.Writer _cbw = new BCFile.Writer(dos, null, "gz", conf, cs);

      SamplerConfigurationImpl samplerConfig =
          SamplerConfigurationImpl.newSamplerConfig(accumuloConfiguration);
      Sampler sampler = null;

      if (samplerConfig != null) {
        sampler = SamplerFactory.newSampler(samplerConfig, accumuloConfiguration);
      }

      writer = new RFile.Writer(_cbw, blockSize, indexBlockSize, samplerConfig, sampler);

      if (startDLG) {
        writer.startDefaultLocalityGroup();
      }
    }

    public void openWriter() throws IOException {
      openWriter(1000);
    }

    public void openWriter(int blockSize) throws IOException {
      openWriter(true, blockSize);
    }

    public void closeWriter() throws IOException {
      dos.flush();
      writer.close();
      dos.close();
      if (baos != null) {
        baos.close();
      }
    }

    public void openReader(Range fence) throws IOException {
      openReader(true, fence);
    }

    public void openReader() throws IOException {
      openReader(true);
    }

    public void openReader(boolean cfsi) throws IOException {
      openReader(cfsi, null);
    }

    public void openReader(boolean cfsi, Range fence) throws IOException {
      int fileLength = 0;
      byte[] data = null;
      data = baos.toByteArray();

      bais = new SeekableByteArrayInputStream(data);
      in = new FSDataInputStream(bais);
      fileLength = data.length;

      DefaultConfiguration dc = DefaultConfiguration.getInstance();
      ConfigurationCopy cc = new ConfigurationCopy(dc);
      cc.set(Property.TSERV_CACHE_MANAGER_IMPL, LruBlockCacheManager.class.getName());
      try {
        manager = BlockCacheManagerFactory.getInstance(cc);
      } catch (ReflectiveOperationException e) {
        throw new IllegalStateException("Error creating BlockCacheManager", e);
      }
      cc.set(Property.TSERV_DEFAULT_BLOCKSIZE, Long.toString(100000));
      cc.set(Property.TSERV_DATACACHE_SIZE, Long.toString(100000000));
      cc.set(Property.TSERV_INDEXCACHE_SIZE, Long.toString(100000000));
      manager.start(BlockCacheConfiguration.forTabletServer(cc));
      LruBlockCache indexCache = (LruBlockCache) manager.getBlockCache(CacheType.INDEX);
      LruBlockCache dataCache = (LruBlockCache) manager.getBlockCache(CacheType.DATA);

      CryptoService cs = CryptoFactoryLoader.getServiceForClient(CryptoEnvironment.Scope.TABLE,
          accumuloConfiguration.getAllCryptoProperties());

      CachableBuilder cb = new CachableBuilder().input(in, "source-1").length(fileLength).conf(conf)
          .cacheProvider(new BasicCacheProvider(indexCache, dataCache)).cryptoService(cs);
      reader = new RFile.Reader(cb);
      if (cfsi) {
        iter = new ColumnFamilySkippingIterator(reader);
      }
      if (fence != null) {
        iter = new FencedReader(reader, fence);
      }

      checkIndex(reader);
    }

    public void closeReader() throws IOException {
      reader.close();
      in.close();
      if (null != manager) {
        manager.stop();
      }
    }

    public void seek(Key nk) throws IOException {
      iter.seek(new Range(nk, null), EMPTY_COL_FAMS, false);
    }
  }

  protected static void checkIndex(Reader reader) throws IOException {
    FileSKVIterator indexIter = reader.getIndex();

    if (indexIter.hasTop()) {
      Key lastKey = new Key(indexIter.getTopKey());

      if (reader.getFirstRow().compareTo(lastKey.getRow()) > 0) {
        throw new IllegalStateException(
            "First key out of order " + reader.getFirstRow() + " " + lastKey);
      }

      indexIter.next();

      while (indexIter.hasTop()) {
        if (lastKey.compareTo(indexIter.getTopKey()) > 0) {
          throw new IllegalStateException(
              "Indext out of order " + lastKey + " " + indexIter.getTopKey());
        }

        lastKey = new Key(indexIter.getTopKey());
        indexIter.next();

      }

      if (!reader.getLastRow().equals(lastKey.getRow())) {
        throw new IllegalStateException(
            "Last key out of order " + reader.getLastRow() + " " + lastKey);
      }
    }
  }

  static Key newKey(String row, String cf, String cq, String cv, long ts) {
    return new Key(row.getBytes(), cf.getBytes(), cq.getBytes(), cv.getBytes(), ts);
  }

  static Value newValue(String val) {
    return new Value(val);
  }

  static String formatString(String prefix, int i) {
    return String.format(prefix + "%06d", i);
  }

  protected void verify(TestRFile trf, Iterator<Key> eki, Iterator<Value> evi) throws IOException {
    verify(trf.iter, eki, evi);
  }

  protected void verify(SortedKeyValueIterator<Key,Value> iter, Iterator<Key> eki,
      Iterator<Value> evi) throws IOException {

    while (iter.hasTop()) {
      Key ek = eki.next();
      Value ev = evi.next();

      assertEquals(ek, iter.getTopKey());
      assertEquals(ev, iter.getTopValue());

      iter.next();
    }

    assertFalse(eki.hasNext());
    assertFalse(evi.hasNext());
  }

  protected void verifyEstimated(FileSKVIterator reader) throws IOException {
    // Test estimated entries for 1 row
    long estimated = reader.estimateOverlappingEntries(new KeyExtent(TableId.of("1"),
        new Text(formatString("r_", 1)), new Text(formatString("r_", 0))));
    // One row contains 256 but the estimate will be more with overlapping index entries
    assertEquals(264, estimated);

    // Test for 2 rows
    estimated = reader.estimateOverlappingEntries(new KeyExtent(TableId.of("1"),
        new Text(formatString("r_", 2)), new Text(formatString("r_", 0))));
    // Two rows contains 512 but the estimate will be more with overlapping index entries
    assertEquals(516, estimated);

    // 3 rows
    // Actual should be 768, estimate is 772
    estimated = reader.estimateOverlappingEntries(
        new KeyExtent(TableId.of("1"), null, new Text(formatString("r_", 0))));
    assertEquals(772, estimated);

    // Tests when full number of entries should return
    estimated = reader.estimateOverlappingEntries(new KeyExtent(TableId.of("1"), null, null));
    assertEquals(1024, estimated);

    estimated = reader.estimateOverlappingEntries(
        new KeyExtent(TableId.of("1"), new Text(formatString("r_", 4)), null));
    assertEquals(1024, estimated);
  }
}
