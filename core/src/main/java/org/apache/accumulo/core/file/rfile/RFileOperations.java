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

import org.apache.accumulo.core.client.sample.Sampler;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.FileSKVWriter;
import org.apache.accumulo.core.file.blockfile.impl.CachableBlockFile;
import org.apache.accumulo.core.file.streams.RateLimitedOutputStream;
import org.apache.accumulo.core.sample.impl.SamplerConfigurationImpl;
import org.apache.accumulo.core.sample.impl.SamplerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Preconditions;

public class RFileOperations extends FileOperations {

  private static final Collection<ByteSequence> EMPTY_CF_SET = Collections.emptySet();

  private static RFile.Reader getReader(FileReaderOperation<?> options) throws IOException {
    CachableBlockFile.Reader _cbr = new CachableBlockFile.Reader(options.getFileSystem(), new Path(options.getFilename()), options.getConfiguration(),
        options.getDataCache(), options.getIndexCache(), options.getRateLimiter(), options.getTableConfiguration());
    return new RFile.Reader(_cbr);
  }

  @Override
  protected long getFileSize(GetFileSizeOperation options) throws IOException {
    return options.getFileSystem().getFileStatus(new Path(options.getFilename())).getLen();
  }

  @Override
  protected FileSKVIterator openIndex(OpenIndexOperation options) throws IOException {
    return getReader(options).getIndex();
  }

  @Override
  protected FileSKVIterator openReader(OpenReaderOperation options) throws IOException {
    RFile.Reader reader = getReader(options);

    if (options.isSeekToBeginning()) {
      reader.seek(new Range((Key) null, null), EMPTY_CF_SET, false);
    }

    return reader;
  }

  @Override
  protected FileSKVIterator openScanReader(OpenScanReaderOperation options) throws IOException {
    RFile.Reader reader = getReader(options);
    reader.seek(options.getRange(), options.getColumnFamilies(), options.isRangeInclusive());
    return reader;
  }

  @Override
  protected FileSKVWriter openWriter(OpenWriterOperation options) throws IOException {

    AccumuloConfiguration acuconf = options.getTableConfiguration();

    long blockSize = acuconf.getAsBytes(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE);
    Preconditions.checkArgument((blockSize < Integer.MAX_VALUE && blockSize > 0), "table.file.compress.blocksize must be greater than 0 and less than "
        + Integer.MAX_VALUE);
    long indexBlockSize = acuconf.getAsBytes(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE_INDEX);
    Preconditions.checkArgument((indexBlockSize < Integer.MAX_VALUE && indexBlockSize > 0),
        "table.file.compress.blocksize.index must be greater than 0 and less than " + Integer.MAX_VALUE);

    SamplerConfigurationImpl samplerConfig = SamplerConfigurationImpl.newSamplerConfig(acuconf);
    Sampler sampler = null;

    if (samplerConfig != null) {
      sampler = SamplerFactory.newSampler(samplerConfig, acuconf, options.isAccumuloStartEnabled());
    }

    String compression = options.getCompression();
    compression = compression == null ? options.getTableConfiguration().get(Property.TABLE_FILE_COMPRESSION_TYPE) : compression;

    FSDataOutputStream outputStream = options.getOutputStream();

    Configuration conf = options.getConfiguration();

    if (outputStream == null) {
      int hrep = conf.getInt("dfs.replication", -1);
      int trep = acuconf.getCount(Property.TABLE_FILE_REPLICATION);
      int rep = hrep;
      if (trep > 0 && trep != hrep) {
        rep = trep;
      }
      long hblock = conf.getLong("dfs.block.size", 1 << 26);
      long tblock = acuconf.getAsBytes(Property.TABLE_FILE_BLOCK_SIZE);
      long block = hblock;
      if (tblock > 0)
        block = tblock;
      int bufferSize = conf.getInt("io.file.buffer.size", 4096);

      String file = options.getFilename();
      FileSystem fs = options.getFileSystem();

      outputStream = fs.create(new Path(file), false, bufferSize, (short) rep, block);
    }

    CachableBlockFile.Writer _cbw = new CachableBlockFile.Writer(new RateLimitedOutputStream(outputStream, options.getRateLimiter()), compression, conf,
        acuconf);

    RFile.Writer writer = new RFile.Writer(_cbw, (int) blockSize, (int) indexBlockSize, samplerConfig, sampler);
    return writer;
  }
}
