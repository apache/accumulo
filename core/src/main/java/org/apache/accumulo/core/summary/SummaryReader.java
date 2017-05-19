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
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.blockfile.cache.BlockCache;
import org.apache.accumulo.core.file.blockfile.cache.CacheEntry;
import org.apache.accumulo.core.file.blockfile.impl.CachableBlockFile;
import org.apache.accumulo.core.file.rfile.RFile.Reader;
import org.apache.accumulo.core.file.rfile.bcfile.MetaBlockDoesNotExist;
import org.apache.accumulo.core.summary.Gatherer.RowRange;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.WritableUtils;

public class SummaryReader {

  private static interface BlockReader {
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
    public CacheEntry cacheBlock(String blockName, byte[] buf, boolean inMemory) {
      return summaryCache.cacheBlock(blockName, buf, inMemory);
    }

    @Override
    public CacheEntry getBlock(String blockName) {
      CacheEntry ce = summaryCache.getBlock(blockName);
      if (ce == null) {
        // Its possible the index cache may have this info, so check there. This is an opportunistic check.
        ce = indexCache.getBlock(blockName);
      }
      return ce;
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

  private static List<SummarySerializer> load(BlockReader bcReader, Predicate<SummarizerConfiguration> summarySelector) throws IOException {

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
            try (DataInputStream summaryIn = bcReader.getMetaBlock(SummaryWriter.METASTORE_PREFIX + "." + block)) {
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

  private static SummaryReader load(CachableBlockFile.Reader bcReader, Predicate<SummarizerConfiguration> summarySelector, SummarizerFactory factory)
      throws IOException {
    SummaryReader fileSummaries = new SummaryReader();
    fileSummaries.summaryStores = load(name -> bcReader.getMetaBlock(name), summarySelector);
    fileSummaries.factory = factory;
    return fileSummaries;
  }

  public static SummaryReader load(Configuration conf, AccumuloConfiguration aConf, InputStream inputStream, long length,
      Predicate<SummarizerConfiguration> summarySelector, SummarizerFactory factory) throws IOException {
    org.apache.accumulo.core.file.blockfile.impl.CachableBlockFile.Reader bcReader = new CachableBlockFile.Reader((InputStream & Seekable) inputStream, length,
        conf, aConf);
    return load(bcReader, summarySelector, factory);
  }

  public static SummaryReader load(FileSystem fs, Configuration conf, AccumuloConfiguration aConf, SummarizerFactory factory, Path file,
      Predicate<SummarizerConfiguration> summarySelector, BlockCache summaryCache, BlockCache indexCache) {
    CachableBlockFile.Reader bcReader = null;

    try {
      // the reason BCFile is used instead of RFile is to avoid reading in the RFile meta block when only summary data is wanted.
      CompositeCache compositeCache = new CompositeCache(summaryCache, indexCache);
      bcReader = new CachableBlockFile.Reader(fs, file, conf, null, compositeCache, aConf);
      return load(bcReader, summarySelector, factory);
    } catch (FileNotFoundException fne) {
      SummaryReader sr = new SummaryReader();
      sr.factory = factory;
      sr.summaryStores = Collections.emptyList();
      sr.deleted = true;
      return sr;
    } catch (IOException e) {
      try {
        if (!fs.exists(file)) {
          SummaryReader sr = new SummaryReader();
          sr.factory = factory;
          sr.summaryStores = Collections.emptyList();
          sr.deleted = true;
          return sr;
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

  private static void print(FileSKVIterator fsi, String indent, PrintStream out) throws IOException {

    out.printf("Summary data : \n");

    List<SummarySerializer> stores = load(name -> fsi.getMetaStore(name), conf -> true);
    int i = 1;
    for (SummarySerializer summaryStore : stores) {
      out.printf("%sSummary %d of %d generated by : %s\n", indent, i, stores.size(), summaryStore.getSummarizerConfiguration());
      i++;
      summaryStore.print(indent, indent, out);
    }
  }

  public static void print(Reader iter, PrintStream out) throws IOException {
    print(iter, "   ", out);
  }

  private static SummarizerConfiguration readConfig(DataInputStream in) throws IOException {
    // read summarizer configuration
    String summarizerClazz = in.readUTF();
    String configId = in.readUTF();
    org.apache.accumulo.core.client.summary.SummarizerConfiguration.Builder scb = SummarizerConfiguration.builder(summarizerClazz).setPropertyId(configId);
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
        initial.add(new SummaryCollection.FileSummary(summaryStore.getSummarizerConfiguration(), summary, exceeded));
      }
    }

    return new SummaryCollection(initial);
  }
}
