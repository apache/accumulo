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
package org.apache.accumulo.core.file;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.spi.cache.BlockCache;
import org.apache.accumulo.core.spi.crypto.CryptoService;
import org.apache.accumulo.core.util.ratelimit.RateLimiter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;

import com.google.common.cache.Cache;

public abstract class FileOperations {

  private static final HashSet<String> validExtensions = new HashSet<>(
      Arrays.asList(Constants.MAPFILE_EXTENSION, RFile.EXTENSION));

  public static Set<String> getValidExtensions() {
    return validExtensions;
  }

  public static String getNewFileExtension(AccumuloConfiguration acuconf) {
    return acuconf.get(Property.TABLE_FILE_TYPE);
  }

  public static FileOperations getInstance() {
    return new DispatchingFileFactory();
  }

  //
  // Abstract methods (to be implemented by subclasses)
  //

  protected abstract long getFileSize(FileStuff options) throws IOException;

  protected abstract FileSKVWriter openWriter(WriterBuilder options) throws IOException;

  protected abstract FileSKVIterator openIndex(IndexReaderBuilder options) throws IOException;

  protected abstract FileSKVIterator openScanReader(ScanReaderBuilder options) throws IOException;

  protected abstract FileSKVIterator openReader(ReaderBuilder options) throws IOException;

  //
  // File operations
  //

  /**
   * Construct an operation object allowing one to create a writer for a file. <br>
   * Syntax:
   *
   * <pre>
   * FileSKVWriter writer = fileOperations.newWriterBuilder()
   *     .forFile(...)
   *     .withTableConfiguration(...)
   *     .withRateLimiter(...) // optional
   *     .withCompression(...) // optional
   *     .build();
   * </pre>
   */
  public WriterBuilder newWriterBuilder() {
    return new WriterBuilder();
  }

  /**
   * Construct an operation object allowing one to create an index iterator for a file. <br>
   * Syntax:
   *
   * <pre>
   * FileSKVIterator iterator = fileOperations.newIndexReaderBuilder()
   *     .forFile(...)
   *     .withTableConfiguration(...)
   *     .withRateLimiter(...) // optional
   *     .withBlockCache(...) // optional
   *     .build();
   * </pre>
   */
  public IndexReaderBuilder newIndexReaderBuilder() {
    return new IndexReaderBuilder();
  }

  /**
   * Construct an operation object allowing one to create a "scan" reader for a file. Scan readers
   * do not have any optimizations for seeking beyond their initial position. This is useful for
   * file operations that only need to scan data within a range and do not need to seek. Therefore
   * file metadata such as indexes does not need to be kept in memory while the file is scanned.
   * Also seek optimizations like bloom filters do not need to be loaded. <br>
   * Syntax:
   *
   * <pre>
   * FileSKVIterator scanner = fileOperations.newScanReaderBuilder()
   *     .forFile(...)
   *     .withTableConfiguration(...)
   *     .overRange(...)
   *     .withRateLimiter(...) // optional
   *     .withBlockCache(...) // optional
   *     .build();
   * </pre>
   */
  @SuppressWarnings("unchecked")
  public ScanReaderBuilder newScanReaderBuilder() {
    return new ScanReaderBuilder();
  }

  /**
   * Construct an operation object allowing one to create a reader for a file. A reader constructed
   * in this manner fully supports seeking, and also enables any optimizations related to seeking
   * (e.g. Bloom filters). <br>
   * Syntax:
   *
   * <pre>
   * FileSKVIterator scanner = fileOperations.newReaderBuilder()
   *     .forFile(...)
   *     .withTableConfiguration(...)
   *     .withRateLimiter(...) // optional
   *     .withBlockCache(...) // optional
   *     .seekToBeginning(...) // optional
   *     .build();
   * </pre>
   */
  public ReaderBuilder newReaderBuilder() {
    return new ReaderBuilder();
  }

  /**
   * Objects common to all FileOperations.
   */
  public static class FileStuff {
    private AccumuloConfiguration tableConfiguration;
    private String filename;
    private FileSystem fs;
    private Configuration fsConf;
    private RateLimiter rateLimiter;

    public FileSystem getFs() {
      return fs;
    }

    public FileStuff fs(FileSystem fs) {
      this.fs = fs;
      return this;
    }

    public Configuration getFsConf() {
      return fsConf;
    }

    public FileStuff fsConf(Configuration fsConf) {
      this.fsConf = fsConf;
      return this;
    }

    protected FileStuff filename(String filename) {
      this.filename = filename;
      return this;
    }

    public String getFilename() {
      return filename;
    }

    public FileSystem getFileSystem() {
      return fs;
    }

    protected FileStuff configuration(Configuration fsConf) {
      this.fsConf = fsConf;
      return this;
    }

    public Configuration getConfiguration() {
      return fsConf;
    }

    public FileStuff tableConfiguration(AccumuloConfiguration tableConfiguration) {
      this.tableConfiguration = tableConfiguration;
      return this;
    }

    public AccumuloConfiguration getTableConfiguration() {
      return tableConfiguration;
    }

    public RateLimiter getRateLimiter() {
      return rateLimiter;
    }

    public FileStuff rateLimiter(RateLimiter rateLimiter) {
      this.rateLimiter = rateLimiter;
      return this;
    }

    /** Check for null parameters. */
    public void validate() {
      Objects.requireNonNull(getFilename());
      Objects.requireNonNull(getFileSystem());
      Objects.requireNonNull(getConfiguration());
      Objects.requireNonNull(getTableConfiguration());
    }
  }

  /**
   * Operation object for constructing a writer.
   */
  public class WriterBuilder extends FileStuff implements WriterTableConfiguration {
    private String compression;
    private FSDataOutputStream outputStream;
    private boolean enableAccumuloStart = true;

    public WriterTableConfiguration forOutputStream(String extension,
        FSDataOutputStream outputStream, Configuration fsConf) {
      this.outputStream = outputStream;
      filename("foo" + extension).configuration(fsConf);
      return this;
    }

    public WriterTableConfiguration forFile(String filename, FileSystem fs, Configuration fsConf) {
      filename(filename).fs(fs).fsConf(fsConf);
      return this;
    }

    public WriterBuilder withTableConfiguration(AccumuloConfiguration tableConfiguration) {
      tableConfiguration(tableConfiguration);
      return this;
    }

    public WriterBuilder withStartDisabled() {
      this.enableAccumuloStart = false;
      return this;
    }

    public WriterBuilder withCompression(String compression) {
      this.compression = compression;
      return this;
    }

    public WriterBuilder withRateLimiter(RateLimiter rateLimiter) {
      rateLimiter(rateLimiter);
      return this;
    }

    public boolean isAccumuloStartEnabled() {
      return enableAccumuloStart;
    }

    public String getCompression() {
      return compression;
    }

    public FSDataOutputStream getOutputStream() {
      return outputStream;
    }

    public FileSKVWriter build() throws IOException {
      if (outputStream == null) {
        validate();
      } else {
        Objects.requireNonNull(getConfiguration());
        Objects.requireNonNull(getTableConfiguration());
      }
      return openWriter(this);
    }
  }

  public interface WriterTableConfiguration {
    public WriterBuilder withTableConfiguration(AccumuloConfiguration tableConfiguration);
  }

  /**
   * Options common to all {@code FileOperations} which perform reads.
   */
  public class ReaderBuilder extends FileStuff implements ReaderTableConfiguration {
    private BlockCache dataCache;
    private BlockCache indexCache;
    private Cache<String,Long> fileLenCache;
    private boolean seekToBeginning = false;
    private CryptoService cryptoService;

    public ReaderTableConfiguration forFile(String filename, FileSystem fs, Configuration fsConf) {
      filename(filename).fs(fs).fsConf(fsConf);
      return this;
    }

    public ReaderBuilder withTableConfiguration(AccumuloConfiguration tableConfiguration) {
      tableConfiguration(tableConfiguration);
      return this;
    }

    /**
     * (Optional) Set the block cache pair to be used to optimize reads within the constructed
     * reader.
     */
    public ReaderBuilder withBlockCache(BlockCache dataCache, BlockCache indexCache) {
      this.dataCache = dataCache;
      this.indexCache = indexCache;
      return this;
    }

    /** (Optional) set the data cache to be used to optimize reads within the constructed reader. */
    public ReaderBuilder withDataCache(BlockCache dataCache) {
      this.dataCache = dataCache;
      return this;
    }

    /**
     * (Optional) set the index cache to be used to optimize reads within the constructed reader.
     */
    public ReaderBuilder withIndexCache(BlockCache indexCache) {
      this.indexCache = indexCache;
      return this;
    }

    public ReaderBuilder withFileLenCache(Cache<String,Long> fileLenCache) {
      this.fileLenCache = fileLenCache;
      return this;
    }

    public ReaderBuilder withCryptoService(CryptoService cryptoService) {
      this.cryptoService = cryptoService;
      return this;
    }

    public ReaderBuilder withRateLimiter(RateLimiter rateLimiter) {
      rateLimiter(rateLimiter);
      return this;
    }

    public BlockCache getDataCache() {
      return dataCache;
    }

    public BlockCache getIndexCache() {
      return indexCache;
    }

    public Cache<String,Long> getFileLenCache() {
      return fileLenCache;
    }

    public CryptoService getCryptoService() {
      return cryptoService;
    }

    /**
     * Seek the constructed iterator to the beginning of its domain before returning. Equivalent to
     * {@code seekToBeginning(true)}.
     */
    public ReaderBuilder seekToBeginning() {
      seekToBeginning(true);
      return this;
    }

    /** If true, seek the constructed iterator to the beginning of its domain before returning. */
    public ReaderBuilder seekToBeginning(boolean seekToBeginning) {
      this.seekToBeginning = seekToBeginning;
      return this;
    }

    public boolean isSeekToBeginning() {
      return seekToBeginning;
    }

    /** Execute the operation, constructing the specified file reader. */
    public FileSKVIterator build() throws IOException {
      validate();
      return openReader(this);
    }
  }

  public interface ReaderTableConfiguration {
    ReaderBuilder withTableConfiguration(AccumuloConfiguration tableConfiguration);
  }

  /**
   * Operation object for opening an index.
   */
  public class IndexReaderBuilder extends FileStuff implements IndexReaderTableConfiguration {

    public IndexReaderTableConfiguration forFile(String filename, FileSystem fs,
        Configuration fsConf) {
      filename(filename).fs(fs).fsConf(fsConf);
      return this;
    }

    public IndexReaderBuilder withTableConfiguration(AccumuloConfiguration tableConfiguration) {
      tableConfiguration(tableConfiguration);
      return this;
    }

    public FileSKVIterator build() throws IOException {
      validate();
      return openIndex(this);
    }
  }

  public interface IndexReaderTableConfiguration {
    IndexReaderBuilder withTableConfiguration(AccumuloConfiguration tableConfiguration);
  }

  /** Operation object for opening a scan reader. */
  public class ScanReaderBuilder extends FileStuff implements ScanReaderTableConfiguration {
    private Range range;
    private Set<ByteSequence> columnFamilies;
    private boolean inclusive;

    public ScanReaderTableConfiguration forFile(String filename, FileSystem fs,
        Configuration fsConf) {
      filename(filename).fs(fs).fsConf(fsConf);
      return this;
    }

    public ScanReaderBuilder withTableConfiguration(AccumuloConfiguration tableConfiguration) {
      tableConfiguration(tableConfiguration);
      return this;
    }

    /** Set the range over which the constructed iterator will search. */
    public ScanReaderBuilder overRange(Range range, Set<ByteSequence> columnFamilies,
        boolean inclusive) {
      this.range = range;
      this.columnFamilies = columnFamilies;
      this.inclusive = inclusive;
      return this;
    }

    /** The range over which this reader should scan. */
    public Range getRange() {
      return range;
    }

    /** The column families which this reader should scan. */
    public Set<ByteSequence> getColumnFamilies() {
      return columnFamilies;
    }

    public boolean isRangeInclusive() {
      return inclusive;
    }

    /** Execute the operation, constructing a scan iterator. */
    public FileSKVIterator build() throws IOException {
      validate();
      Objects.requireNonNull(range);
      Objects.requireNonNull(columnFamilies);
      return openScanReader(this);
    }
  }

  public interface ScanReaderTableConfiguration {
    ScanReaderBuilder withTableConfiguration(AccumuloConfiguration tableConfiguration);
  }

}
