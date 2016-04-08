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
package org.apache.accumulo.core.file.rfile;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.FileSKVWriter;
import org.apache.accumulo.core.file.blockfile.cache.BlockCache;
import org.apache.accumulo.core.file.blockfile.impl.CachableBlockFile;
import org.apache.accumulo.core.file.rfile.RFile.Reader;
import org.apache.accumulo.core.file.rfile.RFile.Writer;
import org.apache.accumulo.core.sample.Sampler;
import org.apache.accumulo.core.sample.impl.SamplerConfigurationImpl;
import org.apache.accumulo.core.sample.impl.SamplerFactory;
import org.apache.accumulo.core.util.RateLimiter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class RFileOperations extends FileOperations {

  private static final Collection<ByteSequence> EMPTY_CF_SET = Collections.emptySet();

  @Override
  public long getFileSize(String file, FileSystem fs, Configuration conf, AccumuloConfiguration acuconf) throws IOException {
    return fs.getFileStatus(new Path(file)).getLen();
  }

  @Override
  public FileSKVIterator openIndex(String file, FileSystem fs, Configuration conf, AccumuloConfiguration acuconf) throws IOException {
    return openIndex(file, fs, conf, acuconf, null, null);
  }

  private static RFile.Reader getReader(String file, FileSystem fs, Configuration conf, AccumuloConfiguration acuconf, BlockCache dataCache,
      BlockCache indexCache) throws IOException {
    Path path = new Path(file);
    CachableBlockFile.Reader _cbr = new CachableBlockFile.Reader(fs, path, conf, dataCache, indexCache, acuconf);
    return new RFile.Reader(_cbr);
  }

  @Override
  public FileSKVIterator openIndex(String file, FileSystem fs, Configuration conf, AccumuloConfiguration acuconf, BlockCache dataCache, BlockCache indexCache)
      throws IOException {
    return getReader(file, fs, conf, acuconf, indexCache, indexCache).getIndex();
  }

  @Override
  public FileSKVIterator openReader(String file, boolean seekToBeginning, FileSystem fs, Configuration conf, RateLimiter readLimiter,
      AccumuloConfiguration acuconf) throws IOException {
    return openReader(file, seekToBeginning, fs, conf, readLimiter, acuconf, null, null);
  }

  @Override
  public FileSKVIterator openReader(String file, boolean seekToBeginning, FileSystem fs, Configuration conf, RateLimiter readLimiter,
      AccumuloConfiguration acuconf, BlockCache dataCache, BlockCache indexCache) throws IOException {
    Path path = new Path(file);

    CachableBlockFile.Reader _cbr = new CachableBlockFile.Reader(fs, path, conf, dataCache, indexCache, readLimiter, acuconf);
    Reader iter = new RFile.Reader(_cbr);

    if (seekToBeginning) {
      iter.seek(new Range((Key) null, null), EMPTY_CF_SET, false);
    }

    return iter;
  }

  @Override
  public FileSKVIterator openReader(String file, Range range, Set<ByteSequence> columnFamilies, boolean inclusive, FileSystem fs, Configuration conf,
      RateLimiter readLimiter, AccumuloConfiguration tableConf) throws IOException {
    FileSKVIterator iter = openReader(file, false, fs, conf, readLimiter, tableConf, null, null);
    iter.seek(range, columnFamilies, inclusive);
    return iter;
  }

  @Override
  public FileSKVIterator openReader(String file, Range range, Set<ByteSequence> columnFamilies, boolean inclusive, FileSystem fs, Configuration conf,
      RateLimiter readLimiter, AccumuloConfiguration tableConf, BlockCache dataCache, BlockCache indexCache) throws IOException {
    FileSKVIterator iter = openReader(file, false, fs, conf, readLimiter, tableConf, dataCache, indexCache);
    iter.seek(range, columnFamilies, inclusive);
    return iter;
  }

  @Override
  public FileSKVWriter openWriter(String file, FileSystem fs, Configuration conf, RateLimiter writeLimiter, AccumuloConfiguration acuconf) throws IOException {
    return openWriter(file, fs, conf, writeLimiter, acuconf, acuconf.get(Property.TABLE_FILE_COMPRESSION_TYPE));
  }

  FileSKVWriter openWriter(String file, FileSystem fs, Configuration conf, RateLimiter writeLimiter, AccumuloConfiguration acuconf, String compression)
      throws IOException {
    int hrep = conf.getInt("dfs.replication", -1);
    int trep = acuconf.getCount(Property.TABLE_FILE_REPLICATION);
    int rep = hrep;
    if (trep > 0 && trep != hrep) {
      rep = trep;
    }
    long hblock = conf.getLong("dfs.block.size", 1 << 26);
    long tblock = acuconf.getMemoryInBytes(Property.TABLE_FILE_BLOCK_SIZE);
    long block = hblock;
    if (tblock > 0)
      block = tblock;
    int bufferSize = conf.getInt("io.file.buffer.size", 4096);

    long blockSize = acuconf.getMemoryInBytes(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE);
    long indexBlockSize = acuconf.getMemoryInBytes(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE_INDEX);

    SamplerConfigurationImpl samplerConfig = SamplerConfigurationImpl.newSamplerConfig(acuconf);
    Sampler sampler = null;

    if (samplerConfig != null) {
      sampler = SamplerFactory.newSampler(samplerConfig, acuconf);
    }

    CachableBlockFile.Writer _cbw = new CachableBlockFile.Writer(fs.create(new Path(file), false, bufferSize, (short) rep, block), compression, writeLimiter,
        conf, acuconf);
    Writer writer = new RFile.Writer(_cbw, (int) blockSize, (int) indexBlockSize, samplerConfig, sampler);
    return writer;
  }
}
