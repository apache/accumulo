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
package org.apache.accumulo.tserver.tablet;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.accumulo.core.classloader.ClassLoaderUtil;
import org.apache.accumulo.core.client.PluginEnvironment;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.CompactionStrategyConfig;
import org.apache.accumulo.core.client.admin.PluginConfig;
import org.apache.accumulo.core.client.admin.compaction.CompactableFile;
import org.apache.accumulo.core.client.admin.compaction.CompactionConfigurer;
import org.apache.accumulo.core.client.admin.compaction.CompactionSelector;
import org.apache.accumulo.core.client.admin.compaction.CompactionSelector.Selection;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.client.summary.SummarizerConfiguration;
import org.apache.accumulo.core.client.summary.Summary;
import org.apache.accumulo.core.clientImpl.CompactionStrategyConfigUtil;
import org.apache.accumulo.core.clientImpl.UserCompactionUtils;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.TabletIdImpl;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.metadata.CompactableFileImpl;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.TabletFile;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.sample.impl.SamplerConfigurationImpl;
import org.apache.accumulo.core.spi.cache.BlockCache;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.core.spi.crypto.CryptoService;
import org.apache.accumulo.core.summary.Gatherer;
import org.apache.accumulo.core.summary.SummarizerFactory;
import org.apache.accumulo.core.summary.SummaryCollection;
import org.apache.accumulo.core.summary.SummaryReader;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.server.ServiceEnvironmentImpl;
import org.apache.accumulo.server.compaction.CompactionStats;
import org.apache.accumulo.server.compaction.FileCompactor;
import org.apache.accumulo.server.compaction.FileCompactor.CompactionCanceledException;
import org.apache.accumulo.server.compaction.FileCompactor.CompactionEnv;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.util.MetadataTableUtil;
import org.apache.accumulo.tserver.compaction.CompactionPlan;
import org.apache.accumulo.tserver.compaction.CompactionStrategy;
import org.apache.accumulo.tserver.compaction.MajorCompactionReason;
import org.apache.accumulo.tserver.compaction.MajorCompactionRequest;
import org.apache.accumulo.tserver.compaction.WriteParameters;
import org.apache.accumulo.tserver.tablet.CompactableImpl.CompactionHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Collections2;

@SuppressWarnings("removal")
public class CompactableUtils {

  private static final Logger log = LoggerFactory.getLogger(CompactableUtils.class);

  private final static Cache<TableId,Boolean> strategyWarningsCache =
      CacheBuilder.newBuilder().maximumSize(1000).build();

  public static Map<StoredTabletFile,Pair<Key,Key>> getFirstAndLastKeys(Tablet tablet,
      Set<StoredTabletFile> allFiles) throws IOException {
    final Map<StoredTabletFile,Pair<Key,Key>> result = new HashMap<>();
    final FileOperations fileFactory = FileOperations.getInstance();
    final VolumeManager fs = tablet.getTabletServer().getVolumeManager();
    final TableConfiguration tableConf = tablet.getTableConfiguration();
    final CryptoService cs = tableConf.getCryptoService();
    for (StoredTabletFile file : allFiles) {
      FileSystem ns = fs.getFileSystemByPath(file.getPath());
      try (FileSKVIterator openReader =
          fileFactory.newReaderBuilder().forFile(file.getPathStr(), ns, ns.getConf(), cs)
              .withTableConfiguration(tableConf).seekToBeginning().build()) {
        Key first = openReader.getFirstKey();
        Key last = openReader.getLastKey();
        result.put(file, new Pair<>(first, last));
      }
    }
    return result;
  }

  public static Set<StoredTabletFile> findChopFiles(KeyExtent extent,
      Map<StoredTabletFile,Pair<Key,Key>> firstAndLastKeys, Collection<StoredTabletFile> allFiles) {
    Set<StoredTabletFile> result = new HashSet<>();

    for (StoredTabletFile file : allFiles) {
      Pair<Key,Key> pair = firstAndLastKeys.get(file);
      Key first = pair.getFirst();
      Key last = pair.getSecond();
      // If first and last are null, it's an empty file. Add it to the compact set so it goes
      // away.
      if ((first == null && last == null) || (first != null && !extent.contains(first.getRow()))
          || (last != null && !extent.contains(last.getRow()))) {
        result.add(file);
      }

    }
    return result;
  }

  static CompactionPlan selectFiles(CompactionKind kind, Tablet tablet,
      SortedMap<StoredTabletFile,DataFileValue> datafiles, CompactionStrategyConfig csc) {

    var trsm = tablet.getTabletResources().getTabletServerResourceManager();

    BlockCache sc = trsm.getSummaryCache();
    BlockCache ic = trsm.getIndexCache();
    Cache<String,Long> fileLenCache = trsm.getFileLenCache();
    MajorCompactionRequest request = new MajorCompactionRequest(tablet.getExtent(),
        CompactableUtils.from(kind), tablet.getTabletServer().getVolumeManager(),
        tablet.getTableConfiguration(), sc, ic, fileLenCache, tablet.getContext());

    request.setFiles(datafiles);

    CompactionStrategy strategy = CompactableUtils.newInstance(tablet.getTableConfiguration(),
        csc.getClassName(), CompactionStrategy.class);
    strategy.init(csc.getOptions());

    try {
      if (strategy.shouldCompact(request)) {
        strategy.gatherInformation(request);
        var plan = strategy.getCompactionPlan(request);

        if (plan == null) {
          return new CompactionPlan();
        }

        log.debug("Selected files using compaction strategy {} {} {} {}",
            strategy.getClass().getSimpleName(), csc.getOptions(), plan.inputFiles,
            plan.deleteFiles);

        plan.validate(datafiles.keySet());

        return plan;
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return new CompactionPlan();
  }

  static Map<String,String> computeOverrides(WriteParameters p) {
    if (p == null) {
      return null;
    }

    Map<String,String> result = new HashMap<>();
    if (p.getHdfsBlockSize() > 0) {
      result.put(Property.TABLE_FILE_BLOCK_SIZE.getKey(), "" + p.getHdfsBlockSize());
    }
    if (p.getBlockSize() > 0) {
      result.put(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE.getKey(), "" + p.getBlockSize());
    }
    if (p.getIndexBlockSize() > 0) {
      result.put(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE_INDEX.getKey(),
          "" + p.getIndexBlockSize());
    }
    if (p.getCompressType() != null) {
      result.put(Property.TABLE_FILE_COMPRESSION_TYPE.getKey(), p.getCompressType());
    }
    if (p.getReplication() != 0) {
      result.put(Property.TABLE_FILE_REPLICATION.getKey(), "" + p.getReplication());
    }
    return result;
  }

  static Map<String,String> computeOverrides(Tablet tablet, Set<CompactableFile> files) {
    var tconf = tablet.getTableConfiguration();

    var configurorClass = tconf.get(Property.TABLE_COMPACTION_CONFIGURER);
    if (configurorClass == null || configurorClass.isBlank()) {
      return null;
    }

    var opts = tconf.getAllPropertiesWithPrefixStripped(Property.TABLE_COMPACTION_CONFIGURER_OPTS);

    return computeOverrides(tablet, files, new PluginConfig(configurorClass, opts));
  }

  static Map<String,String> computeOverrides(Tablet tablet, Set<CompactableFile> files,
      PluginConfig cfg) {
    CompactionConfigurer configurer = CompactableUtils.newInstance(tablet.getTableConfiguration(),
        cfg.getClassName(), CompactionConfigurer.class);

    final ServiceEnvironment senv = new ServiceEnvironmentImpl(tablet.getContext());

    configurer.init(new CompactionConfigurer.InitParameters() {
      @Override
      public Map<String,String> getOptions() {
        return cfg.getOptions();
      }

      @Override
      public PluginEnvironment getEnvironment() {
        return senv;
      }

      @Override
      public TableId getTableId() {
        return tablet.getExtent().tableId();
      }
    });

    var overrides = configurer.override(new CompactionConfigurer.InputParameters() {
      @Override
      public Collection<CompactableFile> getInputFiles() {
        return files;
      }

      @Override
      public PluginEnvironment getEnvironment() {
        return senv;
      }

      @Override
      public TableId getTableId() {
        return tablet.getExtent().tableId();
      }

      @Override
      public TabletId getTabletId() {
        return new TabletIdImpl(tablet.getExtent());
      }
    });

    if (overrides.getOverrides().isEmpty()) {
      return null;
    }

    return overrides.getOverrides();
  }

  static <T> T newInstance(AccumuloConfiguration tableConfig, String className,
      Class<T> baseClass) {
    String context = ClassLoaderUtil.tableContext(tableConfig);
    try {
      return ConfigurationTypeHelper.getClassInstance(context, className, baseClass);
    } catch (IOException | ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }

  static Set<StoredTabletFile> selectFiles(Tablet tablet,
      SortedMap<StoredTabletFile,DataFileValue> datafiles, PluginConfig selectorConfig) {

    CompactionSelector selector = newInstance(tablet.getTableConfiguration(),
        selectorConfig.getClassName(), CompactionSelector.class);

    final ServiceEnvironment senv = new ServiceEnvironmentImpl(tablet.getContext());

    selector.init(new CompactionSelector.InitParameters() {
      @Override
      public Map<String,String> getOptions() {
        return selectorConfig.getOptions();
      }

      @Override
      public PluginEnvironment getEnvironment() {
        return senv;
      }

      @Override
      public TableId getTableId() {
        return tablet.getExtent().tableId();
      }
    });

    Selection selection = selector.select(new CompactionSelector.SelectionParameters() {
      @Override
      public PluginEnvironment getEnvironment() {
        return senv;
      }

      @Override
      public Collection<CompactableFile> getAvailableFiles() {
        return Collections2.transform(datafiles.entrySet(),
            e -> new CompactableFileImpl(e.getKey(), e.getValue()));
      }

      @Override
      public Collection<Summary> getSummaries(Collection<CompactableFile> files,
          Predicate<SummarizerConfiguration> summarySelector) {

        var context = tablet.getContext();
        var tsrm = tablet.getTabletResources().getTabletServerResourceManager();
        var tableConf = tablet.getTableConfiguration();

        SummaryCollection sc = new SummaryCollection();
        SummarizerFactory factory = new SummarizerFactory(tableConf);
        for (CompactableFile cf : files) {
          var file = CompactableFileImpl.toStoredTabletFile(cf);
          FileSystem fs = context.getVolumeManager().getFileSystemByPath(file.getPath());
          Configuration conf = context.getHadoopConf();
          SummaryCollection fsc = SummaryReader
              .load(fs, conf, factory, file.getPath(), summarySelector, tsrm.getSummaryCache(),
                  tsrm.getIndexCache(), tsrm.getFileLenCache(), tableConf.getCryptoService())
              .getSummaries(Collections.singletonList(new Gatherer.RowRange(tablet.getExtent())));
          sc.merge(fsc, factory);
        }

        return sc.getSummaries();
      }

      @Override
      public TableId getTableId() {
        return tablet.getExtent().tableId();
      }

      @Override
      public TabletId getTabletId() {
        return new TabletIdImpl(tablet.getExtent());
      }

      @Override
      public Optional<SortedKeyValueIterator<Key,Value>> getSample(CompactableFile file,
          SamplerConfiguration sc) {
        try {
          FileOperations fileFactory = FileOperations.getInstance();
          Path path = new Path(file.getUri());
          FileSystem ns = tablet.getTabletServer().getVolumeManager().getFileSystemByPath(path);
          var tableConf = tablet.getTableConfiguration();
          var fiter = fileFactory.newReaderBuilder()
              .forFile(path.toString(), ns, ns.getConf(), tableConf.getCryptoService())
              .withTableConfiguration(tableConf).seekToBeginning().build();
          return Optional.ofNullable(fiter.getSample(new SamplerConfigurationImpl(sc)));
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }
    });

    return selection.getFilesToCompact().stream().map(CompactableFileImpl::toStoredTabletFile)
        .collect(Collectors.toSet());
  }

  private static final class TableCompactionHelper implements CompactionHelper {
    private final PluginConfig cselCfg2;
    private final CompactionStrategyConfig stratCfg2;
    private final Tablet tablet;
    private WriteParameters wp;
    private Set<StoredTabletFile> filesToDrop;

    private TableCompactionHelper(PluginConfig cselCfg2, CompactionStrategyConfig stratCfg2,
        Tablet tablet) {
      this.cselCfg2 = cselCfg2;
      this.stratCfg2 = stratCfg2;
      this.tablet = tablet;
    }

    @Override
    public Set<StoredTabletFile> selectFiles(SortedMap<StoredTabletFile,DataFileValue> allFiles) {
      if (cselCfg2 != null) {
        filesToDrop = Set.of();
        return CompactableUtils.selectFiles(tablet, allFiles, cselCfg2);
      } else {
        var plan =
            CompactableUtils.selectFiles(CompactionKind.SELECTOR, tablet, allFiles, stratCfg2);
        this.wp = plan.writeParameters;
        filesToDrop = Set.copyOf(plan.deleteFiles);
        return Set.copyOf(plan.inputFiles);
      }
    }

    @Override
    public Map<String,String> getConfigOverrides(Set<CompactableFile> files) {
      return computeOverrides(wp);
    }

    @Override
    public Set<StoredTabletFile> getFilesToDrop() {
      Preconditions.checkState(filesToDrop != null);
      return filesToDrop;
    }
  }

  private static final class UserCompactionHelper implements CompactionHelper {
    private final CompactionConfig compactionConfig;
    private final Tablet tablet;
    private final Long compactionId;
    private WriteParameters wp;
    private Set<StoredTabletFile> filesToDrop;

    private UserCompactionHelper(CompactionConfig compactionConfig, Tablet tablet,
        Long compactionId) {
      this.compactionConfig = compactionConfig;
      this.tablet = tablet;
      this.compactionId = compactionId;
    }

    @Override
    public Set<StoredTabletFile> selectFiles(SortedMap<StoredTabletFile,DataFileValue> allFiles) {

      Set<StoredTabletFile> selectedFiles;

      if (!CompactionStrategyConfigUtil.isDefault(compactionConfig.getCompactionStrategy())) {
        var plan = CompactableUtils.selectFiles(CompactionKind.USER, tablet, allFiles,
            compactionConfig.getCompactionStrategy());
        this.wp = plan.writeParameters;
        selectedFiles = Set.copyOf(plan.inputFiles);
        filesToDrop = Set.copyOf(plan.deleteFiles);
      } else if (!UserCompactionUtils.isDefault(compactionConfig.getSelector())) {
        selectedFiles =
            CompactableUtils.selectFiles(tablet, allFiles, compactionConfig.getSelector());
        filesToDrop = Set.of();
      } else {
        selectedFiles = allFiles.keySet();
        filesToDrop = Set.of();
      }

      if (selectedFiles.isEmpty()) {
        MetadataTableUtil.updateTabletCompactID(tablet.getExtent(), compactionId,
            tablet.getTabletServer().getContext(), tablet.getTabletServer().getLock());

        tablet.setLastCompactionID(compactionId);
      }

      return selectedFiles;
    }

    @Override
    public Map<String,String> getConfigOverrides(Set<CompactableFile> files) {
      if (!UserCompactionUtils.isDefault(compactionConfig.getConfigurer())) {
        return computeOverrides(tablet, files, compactionConfig.getConfigurer());
      } else if (!CompactionStrategyConfigUtil.isDefault(compactionConfig.getCompactionStrategy())
          && wp != null) {
        return computeOverrides(wp);
      }

      return null;
    }

    @Override
    public Set<StoredTabletFile> getFilesToDrop() {
      Preconditions.checkState(filesToDrop != null);
      return filesToDrop;
    }
  }

  public static CompactionHelper getHelper(CompactionKind kind, Tablet tablet, Long compactionId,
      CompactionConfig compactionConfig) {
    if (kind == CompactionKind.USER) {
      return new UserCompactionHelper(compactionConfig, tablet, compactionId);
    } else if (kind == CompactionKind.SELECTOR) {
      var tconf = tablet.getTableConfiguration();
      var selectorClassName = tconf.get(Property.TABLE_COMPACTION_SELECTOR);

      PluginConfig cselCfg = null;

      if (selectorClassName != null && !selectorClassName.isBlank()) {
        var opts =
            tconf.getAllPropertiesWithPrefixStripped(Property.TABLE_COMPACTION_SELECTOR_OPTS);
        cselCfg = new PluginConfig(selectorClassName, opts);
      }

      CompactionStrategyConfig stratCfg = null;

      if (cselCfg == null && tconf.isPropertySet(Property.TABLE_COMPACTION_STRATEGY)) {
        var stratClassName = tconf.get(Property.TABLE_COMPACTION_STRATEGY);

        try {
          strategyWarningsCache.get(tablet.getExtent().tableId(), () -> {
            log.warn(
                "Table id {} set {} to {}.  Compaction strategies are deprecated.  See the Javadoc"
                    + " for class {} for more details.",
                tablet.getExtent().tableId(), Property.TABLE_COMPACTION_STRATEGY.getKey(),
                stratClassName, CompactionStrategyConfig.class.getName());
            return true;
          });
        } catch (ExecutionException e) {
          throw new RuntimeException(e);
        }

        var opts =
            tconf.getAllPropertiesWithPrefixStripped(Property.TABLE_COMPACTION_STRATEGY_PREFIX);

        stratCfg = new CompactionStrategyConfig(stratClassName).setOptions(opts);
      }

      if (cselCfg != null || stratCfg != null) {
        return new TableCompactionHelper(cselCfg, stratCfg, tablet);
      }
    }

    return null;
  }

  public static Map<String,String> getOverrides(CompactionKind kind, Tablet tablet,
      CompactionHelper driver, Set<CompactableFile> files) {

    Map<String,String> overrides = null;

    if (kind == CompactionKind.USER || kind == CompactionKind.SELECTOR) {
      overrides = driver.getConfigOverrides(files);
    }

    if (overrides == null) {
      overrides = computeOverrides(tablet, files);
    }

    if (overrides == null) {
      return Map.of();
    }

    return overrides;
  }

  private static AccumuloConfiguration getCompactionConfig(TableConfiguration tableConfiguration,
      Map<String,String> overrides) {
    if (overrides.isEmpty()) {
      return tableConfiguration;
    }

    ConfigurationCopy copy = new ConfigurationCopy(tableConfiguration);
    overrides.forEach((k, v) -> copy.set(k, v));

    return copy;
  }

  /**
   * Create the FileCompactor and finally call compact. Returns the Major CompactionStats.
   */
  static CompactionStats compact(Tablet tablet, CompactionJob job,
      CompactableImpl.CompactionInfo cInfo, CompactionEnv cenv,
      Map<StoredTabletFile,DataFileValue> compactFiles, TabletFile tmpFileName)
      throws IOException, CompactionCanceledException {
    TableConfiguration tableConf = tablet.getTableConfiguration();

    AccumuloConfiguration compactionConfig = getCompactionConfig(tableConf,
        getOverrides(job.getKind(), tablet, cInfo.localHelper, job.getFiles()));

    FileCompactor compactor = new FileCompactor(tablet.getContext(), tablet.getExtent(),
        compactFiles, tmpFileName, cInfo.propagateDeletes, cenv, cInfo.iters, compactionConfig,
        tableConf.getCryptoService());

    return compactor.call();
  }

  /**
   * Finish major compaction by bringing the new file online and returning the completed file.
   */
  static Optional<StoredTabletFile> bringOnline(DatafileManager datafileManager,
      CompactableImpl.CompactionInfo cInfo, CompactionStats stats,
      Map<StoredTabletFile,DataFileValue> compactFiles,
      SortedMap<StoredTabletFile,DataFileValue> allFiles, CompactionKind kind,
      TabletFile compactTmpName) throws IOException {
    if (kind == CompactionKind.USER || kind == CompactionKind.SELECTOR) {
      cInfo.localHelper.getFilesToDrop().forEach(f -> {
        if (allFiles.containsKey(f)) {
          compactFiles.put(f, allFiles.get(f));
        }
      });
    }
    var dfv = new DataFileValue(stats.getFileSize(), stats.getEntriesWritten());
    return datafileManager.bringMajorCompactionOnline(compactFiles.keySet(), compactTmpName,
        cInfo.checkCompactionId, cInfo.selectedFiles, dfv, Optional.empty());
  }

  public static MajorCompactionReason from(CompactionKind ck) {
    switch (ck) {
      case CHOP:
        return MajorCompactionReason.CHOP;
      case SYSTEM:
      case SELECTOR:
        return MajorCompactionReason.NORMAL;
      case USER:
        return MajorCompactionReason.USER;
      default:
        throw new IllegalArgumentException("Unknown kind " + ck);
    }
  }

  public static TabletFile computeCompactionFileDest(TabletFile tmpFile) {
    String newFilePath = tmpFile.getMetaInsert();
    int idx = newFilePath.indexOf("_tmp");
    if (idx > 0) {
      newFilePath = newFilePath.substring(0, idx);
    } else {
      throw new IllegalArgumentException(
          "Expected compaction tmp file " + tmpFile.getMetaInsert() + " to have suffix '_tmp'");
    }
    return new TabletFile(new Path(newFilePath));
  }

}
