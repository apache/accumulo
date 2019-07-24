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
import org.apache.hadoop.mapred.FileOutputCommitter;

import com.google.common.cache.Cache;

public abstract class FileOperations {

  private static final String HADOOP_JOBHISTORY_LOCATION = "_logs"; // dir related to
                                                                    // hadoop.job.history.user.location

  private static final HashSet<String> validExtensions =
      new HashSet<>(Arrays.asList(Constants.MAPFILE_EXTENSION, RFile.EXTENSION));

  // Sometimes we want to know what files accumulo bulk processing creates
  private static final HashSet<String> bulkWorkingFiles =
      new HashSet<>(Arrays.asList(Constants.BULK_LOAD_MAPPING, Constants.BULK_RENAME_FILE,
          FileOutputCommitter.SUCCEEDED_FILE_NAME, HADOOP_JOBHISTORY_LOCATION));

  public static Set<String> getValidExtensions() {
    return validExtensions;
  }

  public static Set<String> getBulkWorkingFiles() {
    return bulkWorkingFiles;
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

  protected abstract long getFileSize(FileOptions options) throws IOException;

  protected abstract FileSKVWriter openWriter(FileOptions options) throws IOException;

  protected abstract FileSKVIterator openIndex(FileOptions options) throws IOException;

  protected abstract FileSKVIterator openScanReader(FileOptions options) throws IOException;

  protected abstract FileSKVIterator openReader(FileOptions options) throws IOException;

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

  public class FileOptions {
    // objects used by all
    public final AccumuloConfiguration tableConfiguration;
    public final String filename;
    public final FileSystem fs;
    public final Configuration fsConf;
    public final RateLimiter rateLimiter;
    // writer only objects
    public final String compression;
    public final FSDataOutputStream outputStream;
    public final boolean enableAccumuloStart;
    // reader only objects
    public final BlockCache dataCache;
    public final BlockCache indexCache;
    public final Cache<String,Long> fileLenCache;
    public final boolean seekToBeginning;
    public final CryptoService cryptoService;
    // scan reader only objects
    public final Range range;
    public final Set<ByteSequence> columnFamilies;
    public final boolean inclusive;

    public FileOptions(AccumuloConfiguration tableConfiguration, String filename, FileSystem fs,
        Configuration fsConf, RateLimiter rateLimiter, String compression,
        FSDataOutputStream outputStream, boolean enableAccumuloStart, BlockCache dataCache,
        BlockCache indexCache, Cache<String,Long> fileLenCache, boolean seekToBeginning,
        CryptoService cryptoService, Range range, Set<ByteSequence> columnFamilies,
        boolean inclusive) {
      this.tableConfiguration = tableConfiguration;
      this.filename = filename;
      this.fs = fs;
      this.fsConf = fsConf;
      this.rateLimiter = rateLimiter;
      this.compression = compression;
      this.outputStream = outputStream;
      this.enableAccumuloStart = enableAccumuloStart;
      this.dataCache = dataCache;
      this.indexCache = indexCache;
      this.fileLenCache = fileLenCache;
      this.seekToBeginning = seekToBeginning;
      this.cryptoService = Objects.requireNonNull(cryptoService);
      this.range = range;
      this.columnFamilies = columnFamilies;
      this.inclusive = inclusive;
    }

    public AccumuloConfiguration getTableConfiguration() {
      return tableConfiguration;
    }

    public String getFilename() {
      return filename;
    }

    public FileSystem getFileSystem() {
      return fs;
    }

    public Configuration getConfiguration() {
      return fsConf;
    }

    public RateLimiter getRateLimiter() {
      return rateLimiter;
    }

    public String getCompression() {
      return compression;
    }

    public FSDataOutputStream getOutputStream() {
      return outputStream;
    }

    public boolean isAccumuloStartEnabled() {
      return enableAccumuloStart;
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

    public boolean isSeekToBeginning() {
      return seekToBeginning;
    }

    public CryptoService getCryptoService() {
      return cryptoService;
    }

    public Range getRange() {
      return range;
    }

    public Set<ByteSequence> getColumnFamilies() {
      return columnFamilies;
    }

    public boolean isRangeInclusive() {
      return inclusive;
    }
  }

  /**
   * Helper class extended by both writers and readers.
   */
  public class FileHelper {
    private AccumuloConfiguration tableConfiguration;
    private String filename;
    private FileSystem fs;
    private Configuration fsConf;
    private RateLimiter rateLimiter;
    private CryptoService cryptoService;

    protected FileHelper fs(FileSystem fs) {
      this.fs = Objects.requireNonNull(fs);
      return this;
    }

    protected FileHelper fsConf(Configuration fsConf) {
      this.fsConf = Objects.requireNonNull(fsConf);
      return this;
    }

    protected FileHelper filename(String filename) {
      this.filename = Objects.requireNonNull(filename);
      return this;
    }

    protected FileHelper tableConfiguration(AccumuloConfiguration tableConfiguration) {
      this.tableConfiguration = Objects.requireNonNull(tableConfiguration);
      return this;
    }

    protected FileHelper rateLimiter(RateLimiter rateLimiter) {
      this.rateLimiter = rateLimiter;
      return this;
    }

    protected FileHelper cryptoService(CryptoService cs) {
      this.cryptoService = Objects.requireNonNull(cs);
      return this;
    }

    protected FileOptions toWriterBuilderOptions(String compression,
        FSDataOutputStream outputStream, boolean startEnabled) {
      return new FileOptions(tableConfiguration, filename, fs, fsConf, rateLimiter, compression,
          outputStream, startEnabled, null, null, null, false, cryptoService, null, null, true);
    }

    protected FileOptions toReaderBuilderOptions(BlockCache dataCache, BlockCache indexCache,
        Cache<String,Long> fileLenCache, boolean seekToBeginning) {
      return new FileOptions(tableConfiguration, filename, fs, fsConf, rateLimiter, null, null,
          false, dataCache, indexCache, fileLenCache, seekToBeginning, cryptoService, null, null,
          true);
    }

    protected FileOptions toIndexReaderBuilderOptions(Cache<String,Long> fileLenCache) {
      return new FileOptions(tableConfiguration, filename, fs, fsConf, rateLimiter, null, null,
          false, null, null, fileLenCache, false, cryptoService, null, null, true);
    }

    protected FileOptions toScanReaderBuilderOptions(Range range, Set<ByteSequence> columnFamilies,
        boolean inclusive) {
      return new FileOptions(tableConfiguration, filename, fs, fsConf, rateLimiter, null, null,
          false, null, null, null, false, cryptoService, range, columnFamilies, inclusive);
    }

    protected AccumuloConfiguration getTableConfiguration() {
      return tableConfiguration;
    }
  }

  /**
   * Operation object for constructing a writer.
   */
  public class WriterBuilder extends FileHelper implements WriterTableConfiguration {
    private String compression;
    private FSDataOutputStream outputStream;
    private boolean enableAccumuloStart = true;

    public WriterTableConfiguration forOutputStream(String extension,
        FSDataOutputStream outputStream, Configuration fsConf, CryptoService cs) {
      this.outputStream = outputStream;
      filename("foo" + extension).fsConf(fsConf).cryptoService(cs);
      return this;
    }

    public WriterTableConfiguration forFile(String filename, FileSystem fs, Configuration fsConf,
        CryptoService cs) {
      filename(filename).fs(fs).fsConf(fsConf).cryptoService(cs);
      return this;
    }

    @Override
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

    public FileSKVWriter build() throws IOException {
      return openWriter(toWriterBuilderOptions(compression, outputStream, enableAccumuloStart));
    }
  }

  public interface WriterTableConfiguration {
    public WriterBuilder withTableConfiguration(AccumuloConfiguration tableConfiguration);
  }

  /**
   * Options common to all {@code FileOperations} which perform reads.
   */
  public class ReaderBuilder extends FileHelper implements ReaderTableConfiguration {
    private BlockCache dataCache;
    private BlockCache indexCache;
    private Cache<String,Long> fileLenCache;
    private boolean seekToBeginning = false;

    public ReaderTableConfiguration forFile(String filename, FileSystem fs, Configuration fsConf,
        CryptoService cs) {
      filename(filename).fs(fs).fsConf(fsConf).cryptoService(cs);
      return this;
    }

    @Override
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

    public ReaderBuilder withRateLimiter(RateLimiter rateLimiter) {
      rateLimiter(rateLimiter);
      return this;
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

    /** Execute the operation, constructing the specified file reader. */
    public FileSKVIterator build() throws IOException {
      /**
       * If the table configuration disallows caching, rewrite the options object to not pass the
       * caches.
       */
      if (!getTableConfiguration().getBoolean(Property.TABLE_INDEXCACHE_ENABLED)) {
        withIndexCache(null);
      }
      if (!getTableConfiguration().getBoolean(Property.TABLE_BLOCKCACHE_ENABLED)) {
        withDataCache(null);
      }
      return openReader(
          toReaderBuilderOptions(dataCache, indexCache, fileLenCache, seekToBeginning));
    }
  }

  public interface ReaderTableConfiguration {
    ReaderBuilder withTableConfiguration(AccumuloConfiguration tableConfiguration);
  }

  /**
   * Operation object for opening an index.
   */
  public class IndexReaderBuilder extends FileHelper implements IndexReaderTableConfiguration {

    private Cache<String,Long> fileLenCache = null;

    public IndexReaderTableConfiguration forFile(String filename, FileSystem fs,
        Configuration fsConf, CryptoService cs) {
      filename(filename).fs(fs).fsConf(fsConf).cryptoService(cs);
      return this;
    }

    @Override
    public IndexReaderBuilder withTableConfiguration(AccumuloConfiguration tableConfiguration) {
      tableConfiguration(tableConfiguration);
      return this;
    }

    public IndexReaderBuilder withFileLenCache(Cache<String,Long> fileLenCache) {
      this.fileLenCache = fileLenCache;
      return this;
    }

    public FileSKVIterator build() throws IOException {
      return openIndex(toIndexReaderBuilderOptions(fileLenCache));
    }
  }

  public interface IndexReaderTableConfiguration {
    IndexReaderBuilder withTableConfiguration(AccumuloConfiguration tableConfiguration);
  }

  /** Operation object for opening a scan reader. */
  public class ScanReaderBuilder extends FileHelper implements ScanReaderTableConfiguration {
    private Range range;
    private Set<ByteSequence> columnFamilies;
    private boolean inclusive;

    public ScanReaderTableConfiguration forFile(String filename, FileSystem fs,
        Configuration fsConf, CryptoService cs) {
      filename(filename).fs(fs).fsConf(fsConf).cryptoService(cs);
      return this;
    }

    @Override
    public ScanReaderBuilder withTableConfiguration(AccumuloConfiguration tableConfiguration) {
      tableConfiguration(tableConfiguration);
      return this;
    }

    /** Set the range over which the constructed iterator will search. */
    public ScanReaderBuilder overRange(Range range, Set<ByteSequence> columnFamilies,
        boolean inclusive) {
      Objects.requireNonNull(range);
      Objects.requireNonNull(columnFamilies);
      this.range = range;
      this.columnFamilies = columnFamilies;
      this.inclusive = inclusive;
      return this;
    }

    /** Execute the operation, constructing a scan iterator. */
    public FileSKVIterator build() throws IOException {
      return openScanReader(toScanReaderBuilderOptions(range, columnFamilies, inclusive));
    }
  }

  public interface ScanReaderTableConfiguration {
    ScanReaderBuilder withTableConfiguration(AccumuloConfiguration tableConfiguration);
  }
}
