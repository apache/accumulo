/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.summary;

import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import org.apache.accumulo.core.client.summary.SummarizerConfiguration;
import org.apache.accumulo.core.file.blockfile.impl.BasicCacheProvider;
import org.apache.accumulo.core.file.blockfile.impl.CachableBlockFile;
import org.apache.accumulo.core.file.blockfile.impl.CachableBlockFile.CachableBuilder;
import org.apache.accumulo.core.file.rfile.RFile.Reader;
import org.apache.accumulo.core.file.rfile.bcfile.MetaBlockDoesNotExist;
import org.apache.accumulo.core.spi.cache.BlockCache;
import org.apache.accumulo.core.spi.cache.CacheEntry;
import org.apache.accumulo.core.spi.crypto.CryptoService;
import org.apache.accumulo.core.summary.Gatherer.RowRange;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableUtils;

import com.google.common.cache.Cache;

public class SummaryReader {

  private interface BlockReader {
    DataInputStream getMetaBlock(String name) throws IOException;
  }

  private static class CompositeCache implements BlockCache {

    private BlockCache summaryCache;
    private BlockCache indexCache;

    CompositeCache(BlockCache summaryCache, BlockCache indexCache) {
      this.summaryCache = summaryCache;
      this.indexCache = indexCache;
    }

    @Override
    public CacheEntry cacheBlock(String blockName, byte[] buf) {
      return summaryCache.cacheBlock(blockName, buf);
    }

    @Override
    public CacheEntry getBlock(String blockName) {
      CacheEntry ce = summaryCache.getBlock(blockName);
      if (ce == null) {
        // Its possible the index cache may have this info, so check there. This is an opportunistic
        // check.
        ce = indexCache.getBlock(blockName);
      }
      return ce;
    }

    @Override
    public CacheEntry getBlock(String blockName, Loader loader) {
      Loader idxLoader = new Loader() {

        CacheEntry idxCacheEntry;

        @Override
        public Map<String,Loader> getDependencies() {
          idxCacheEntry = indexCache.getBlock(blockName);
          if (idxCacheEntry == null) {
            return loader.getDependencies();
          } else {
            return Collections.emptyMap();
          }
        }

        @Override
        public byte[] load(int maxSize, Map<String,byte[]> dependencies) {
          if (idxCacheEntry == null) {
            return loader.load(maxSize, dependencies);
          } else {
            return idxCacheEntry.getBuffer();
          }
        }
      };
      return summaryCache.getBlock(blockName, idxLoader);
    }

    @Override
    public long getMaxSize() {
      return summaryCache.getMaxSize();
    }

    @Override
    public Stats getStats() {
      return summaryCache.getStats();
    }

    @Override
    public long getMaxHeapSize() {
      return summaryCache.getMaxHeapSize();
    }
  }

  private static List<SummarySerializer> load(BlockReader bcReader,
      Predicate<SummarizerConfiguration> summarySelector) throws IOException {

    try (DataInputStream in = bcReader.getMetaBlock(SummaryWriter.METASTORE_INDEX)) {
      List<SummarySerializer> stores = new ArrayList<>();

      readHeader(in);
      int numSummaries = WritableUtils.readVInt(in);
      for (int i = 0; i < numSummaries; i++) {
        SummarizerConfiguration conf = readConfig(in);
        boolean inline = in.readBoolean();
        if (inline) {
          if (summarySelector.test(conf)) {
            stores.add(SummarySerializer.load(conf, in));
          } else {
            SummarySerializer.skip(in);
          }
        } else {
          int block = WritableUtils.readVInt(in);
          int offset = WritableUtils.readVInt(in);
          if (summarySelector.test(conf)) {
            try (DataInputStream summaryIn =
                bcReader.getMetaBlock(SummaryWriter.METASTORE_PREFIX + "." + block)) {
              long skipped = in.skip(offset);
              while (skipped < offset) {
                skipped += in.skip(offset - skipped);
              }
              stores.add(SummarySerializer.load(conf, summaryIn));
            } catch (MetaBlockDoesNotExist e) {
              // this is unexpected
              throw new IOException(e);
            }
          }
        }
      }

      return stores;
    } catch (MetaBlockDoesNotExist e) {
      return Collections.emptyList();
    }
  }

  private static SummaryReader load(CachableBlockFile.Reader bcReader,
      Predicate<SummarizerConfiguration> summarySelector, SummarizerFactory factory)
      throws IOException {
    SummaryReader fileSummaries = new SummaryReader();
    fileSummaries.summaryStores = load(bcReader::getMetaBlock, summarySelector);
    fileSummaries.factory = factory;
    return fileSummaries;
  }

  public static SummaryReader load(Configuration conf, InputStream inputStream, long length,
      Predicate<SummarizerConfiguration> summarySelector, SummarizerFactory factory,
      List<CryptoService> decrypters) throws IOException {
    CachableBuilder cb =
        new CachableBuilder().input(inputStream).length(length).conf(conf).decrypt(decrypters);
    return load(new CachableBlockFile.Reader(cb), summarySelector, factory);
  }

  public static SummaryReader load(FileSystem fs, Configuration conf, SummarizerFactory factory,
      Path file, Predicate<SummarizerConfiguration> summarySelector, BlockCache summaryCache,
      BlockCache indexCache, Cache<String,Long> fileLenCache, List<CryptoService> decrypters) {
    CachableBlockFile.Reader bcReader = null;

    try {
      // the reason BCFile is used instead of RFile is to avoid reading in the RFile meta block when
      // only summary data is wanted.
      CompositeCache compositeCache = new CompositeCache(summaryCache, indexCache);
      CachableBuilder cb = new CachableBuilder().fsPath(fs, file).conf(conf).fileLen(fileLenCache)
          .cacheProvider(new BasicCacheProvider(compositeCache, null)).decrypt(decrypters);
      bcReader = new CachableBlockFile.Reader(cb);
      return load(bcReader, summarySelector, factory);
    } catch (FileNotFoundException fne) {
      return getEmptyReader(factory);
    } catch (IOException e) {
      try {
        if (!fs.exists(file)) {
          return getEmptyReader(factory);
        }
      } catch (IOException e1) {}
      throw new UncheckedIOException(e);
    } finally {
      if (bcReader != null) {
        try {
          bcReader.close();
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }
    }

  }

  private static SummaryReader getEmptyReader(SummarizerFactory factory) {
    SummaryReader sr = new SummaryReader();
    sr.factory = factory;
    sr.summaryStores = Collections.emptyList();
    sr.deleted = true;
    return sr;
  }

  public static void print(Reader iter, PrintStream out) throws IOException {
    String indent = "   ";
    out.print("Summary data : \n");

    List<SummarySerializer> stores = load(iter::getMetaStore, conf -> true);
    int i = 1;
    for (SummarySerializer summaryStore : stores) {
      out.printf("%sSummary %d of %d generated by : %s\n", indent, i, stores.size(),
          summaryStore.getSummarizerConfiguration());
      i++;
      summaryStore.print(indent, indent, out);
    }
  }

  private static SummarizerConfiguration readConfig(DataInputStream in) throws IOException {
    // read summarizer configuration
    String summarizerClazz = in.readUTF();
    String configId = in.readUTF();
    org.apache.accumulo.core.client.summary.SummarizerConfiguration.Builder scb =
        SummarizerConfiguration.builder(summarizerClazz).setPropertyId(configId);
    int numOpts = WritableUtils.readVInt(in);
    for (int i = 0; i < numOpts; i++) {
      String k = in.readUTF();
      String v = in.readUTF();
      scb.addOption(k, v);
    }

    return scb.build();
  }

  private static byte readHeader(DataInputStream in) throws IOException {
    long magic = in.readLong();
    if (magic != SummaryWriter.MAGIC) {
      throw new IOException("Bad magic : " + String.format("%x", magic));
    }

    byte ver = in.readByte();
    if (ver != SummaryWriter.VER) {
      throw new IOException("Unknown version : " + ver);
    }

    return ver;
  }

  private List<SummarySerializer> summaryStores;

  private SummarizerFactory factory;

  private boolean deleted;

  public SummaryCollection getSummaries(List<RowRange> ranges) {

    List<SummaryCollection.FileSummary> initial = new ArrayList<>();
    if (deleted) {
      return new SummaryCollection(initial, true);
    }
    for (SummarySerializer summaryStore : summaryStores) {
      if (summaryStore.exceededMaxSize()) {
        initial.add(new SummaryCollection.FileSummary(summaryStore.getSummarizerConfiguration()));
      } else {
        Map<String,Long> summary = summaryStore.getSummary(ranges, factory);
        boolean exceeded = summaryStore.exceedsRange(ranges);
        initial.add(new SummaryCollection.FileSummary(summaryStore.getSummarizerConfiguration(),
            summary, exceeded));
      }
    }

    return new SummaryCollection(initial);
  }
}
