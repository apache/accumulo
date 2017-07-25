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
package org.apache.accumulo.tserver.compaction;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.mapred.AccumuloFileOutputFormat;
import org.apache.accumulo.core.client.rfile.RFile.WriterOptions;
import org.apache.accumulo.core.client.summary.Summarizer;
import org.apache.accumulo.core.client.summary.Summarizer.Combiner;
import org.apache.accumulo.core.client.summary.SummarizerConfiguration;
import org.apache.accumulo.core.client.summary.Summary;
import org.apache.accumulo.core.client.summary.Summary.FileStatistics;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.data.impl.TabletIdImpl;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.blockfile.cache.BlockCache;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.summary.Gatherer;
import org.apache.accumulo.core.summary.SummarizerFactory;
import org.apache.accumulo.core.summary.SummaryCollection;
import org.apache.accumulo.core.summary.SummaryReader;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.server.fs.FileRef;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.tserver.compaction.strategies.TooManyDeletesCompactionStrategy;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import com.google.common.base.Preconditions;

/**
 * Information that can be used to determine how a tablet is to be major compacted, if needed.
 */
public class MajorCompactionRequest implements Cloneable {
  final private KeyExtent extent;
  final private MajorCompactionReason reason;
  final private VolumeManager volumeManager;
  final private AccumuloConfiguration tableConfig;
  final private BlockCache indexCache;
  final private BlockCache summaryCache;
  private Map<FileRef,DataFileValue> files;

  public MajorCompactionRequest(KeyExtent extent, MajorCompactionReason reason, VolumeManager manager, AccumuloConfiguration tabletConfig,
      BlockCache summaryCache, BlockCache indexCache) {
    this.extent = extent;
    this.reason = reason;
    this.volumeManager = manager;
    this.tableConfig = tabletConfig;
    this.files = Collections.emptyMap();
    this.summaryCache = summaryCache;
    this.indexCache = indexCache;
  }

  public MajorCompactionRequest(KeyExtent extent, MajorCompactionReason reason, AccumuloConfiguration tabletConfig) {
    this(extent, reason, null, tabletConfig, null, null);
  }

  public MajorCompactionRequest(MajorCompactionRequest mcr) {
    this(mcr.extent, mcr.reason, mcr.volumeManager, mcr.tableConfig, mcr.summaryCache, mcr.indexCache);
    // know this is already unmodifiable, no need to wrap again
    this.files = mcr.files;
  }

  public TabletId getTabletId() {
    return new TabletIdImpl(extent);
  }

  public MajorCompactionReason getReason() {
    return reason;
  }

  public Map<FileRef,DataFileValue> getFiles() {
    return files;
  }

  /**
   * Returns all summaries present in each file.
   *
   * <p>
   * This method can only be called from {@link CompactionStrategy#gatherInformation(MajorCompactionRequest)}. Unfortunately, {@code gatherInformation()} is not
   * called before {@link CompactionStrategy#shouldCompact(MajorCompactionRequest)}. Therefore {@code shouldCompact()) should just return true when a compactions strategy
   * wants to use summary information.
   *
   * <p>
   * When using summaries to make compaction decisions, its important to ensure that all summary data fits in the tablet server summary cache. The size of this
   * cache is configured by code tserver.cache.summary.size}. Also its important to use the summarySelector predicate to only retrieve the needed summary data.
   * Otherwise uneeded summary data could be brought into the cache.
   *
   * <p>
   * Some files may contain data outside of a tablets range. When {@link Summarizer}'s generate small amounts of summary data, multiple summaries may be stored
   * within a file for different row ranges. This will allow more accurate summaries to be returned for the case where a file has data outside a tablets range.
   * However, some summary data outside of the tablets range may still be included. When this happens {@link FileStatistics#getExtra()} will be non zero. Also,
   * its good to be aware of the other potential causes of inaccuracies {@link FileStatistics#getInaccurate()}
   *
   * <p>
   * When this method is called with multiple files, it will automatically merge summary data using {@link Combiner#merge(Map, Map)}. If summary information is
   * needed for each file, then just call this method for each file.
   *
   * <p>
   * Writing a compaction strategy that uses summary information is a bit tricky. See the source code for {@link TooManyDeletesCompactionStrategy} as an example
   * of a compaction strategy.
   *
   * @see Summarizer
   * @see TableOperations#addSummarizers(String, SummarizerConfiguration...)
   * @see AccumuloFileOutputFormat#setSummarizers(org.apache.hadoop.mapred.JobConf, SummarizerConfiguration...)
   * @see WriterOptions#withSummarizers(SummarizerConfiguration...)
   */
  public List<Summary> getSummaries(Collection<FileRef> files, Predicate<SummarizerConfiguration> summarySelector) throws IOException {
    Preconditions.checkState(volumeManager != null,
        "Getting summaries is not supported at this time.  Its only supported when CompactionStrategy.gatherInformation() is called.");
    SummaryCollection sc = new SummaryCollection();
    SummarizerFactory factory = new SummarizerFactory(tableConfig);
    for (FileRef file : files) {
      FileSystem fs = volumeManager.getVolumeByPath(file.path()).getFileSystem();
      Configuration conf = CachedConfiguration.getInstance();
      SummaryCollection fsc = SummaryReader.load(fs, conf, tableConfig, factory, file.path(), summarySelector, summaryCache, indexCache).getSummaries(
          Collections.singletonList(new Gatherer.RowRange(extent)));
      sc.merge(fsc, factory);
    }

    return sc.getSummaries();
  }

  public void setFiles(Map<FileRef,DataFileValue> update) {
    this.files = Collections.unmodifiableMap(update);
  }

  public FileSKVIterator openReader(FileRef ref) throws IOException {
    Preconditions.checkState(volumeManager != null,
        "Opening files is not supported at this time.  Its only supported when CompactionStrategy.gatherInformation() is called.");
    // @TODO verify the file isn't some random file in HDFS
    // @TODO ensure these files are always closed?
    FileOperations fileFactory = FileOperations.getInstance();
    FileSystem ns = volumeManager.getVolumeByPath(ref.path()).getFileSystem();
    FileSKVIterator openReader = fileFactory.newReaderBuilder().forFile(ref.path().toString(), ns, ns.getConf()).withTableConfiguration(tableConfig)
        .seekToBeginning().build();
    return openReader;
  }

  public Map<String,String> getTableProperties() {
    return tableConfig.getAllPropertiesWithPrefix(Property.TABLE_PREFIX);
  }

  public String getTableConfig(String key) {
    Property property = Property.getPropertyByKey(key);
    if (property == null || property.isSensitive())
      throw new RuntimeException("Unable to access the configuration value " + key);
    return tableConfig.get(property);
  }

  public int getMaxFilesPerTablet() {
    return tableConfig.getMaxFilesPerTablet();
  }

  @Override
  public MajorCompactionRequest clone() throws CloneNotSupportedException {
    return (MajorCompactionRequest) super.clone();
  }
}
