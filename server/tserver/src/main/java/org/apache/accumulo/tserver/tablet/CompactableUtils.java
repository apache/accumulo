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
package org.apache.accumulo.tserver.tablet;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.PluginEnvironment;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.CompactionConfigurerConfig;
import org.apache.accumulo.core.client.admin.CompactionSelectorConfig;
import org.apache.accumulo.core.client.admin.CompactionStrategyConfig;
import org.apache.accumulo.core.client.admin.compaction.CompactableFile;
import org.apache.accumulo.core.client.admin.compaction.CompactionConfigurer;
import org.apache.accumulo.core.client.admin.compaction.CompactionSelector;
import org.apache.accumulo.core.client.admin.compaction.CompactionSelector.Selection;
import org.apache.accumulo.core.client.summary.SummarizerConfiguration;
import org.apache.accumulo.core.client.summary.Summary;
import org.apache.accumulo.core.clientImpl.UserCompactionUtils;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.AccumuloConfiguration.Deriver;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.metadata.CompactableFileImpl;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.TabletFile;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.spi.cache.BlockCache;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.accumulo.core.spi.compaction.CompactionDispatcher;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.core.summary.Gatherer;
import org.apache.accumulo.core.summary.SummarizerFactory;
import org.apache.accumulo.core.summary.SummaryCollection;
import org.apache.accumulo.core.summary.SummaryReader;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.ratelimit.RateLimiter;
import org.apache.accumulo.server.ServiceEnvironmentImpl;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.util.MetadataTableUtil;
import org.apache.accumulo.tserver.compaction.CompactionPlan;
import org.apache.accumulo.tserver.compaction.CompactionStrategy;
import org.apache.accumulo.tserver.compaction.MajorCompactionReason;
import org.apache.accumulo.tserver.compaction.MajorCompactionRequest;
import org.apache.accumulo.tserver.compaction.WriteParameters;
import org.apache.accumulo.tserver.tablet.CompactableImpl.CompactionHelper;
import org.apache.accumulo.tserver.tablet.Compactor.CompactionCanceledException;
import org.apache.accumulo.tserver.tablet.Compactor.CompactionEnv;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;

public class CompactableUtils {

  private static final Logger log = LoggerFactory.getLogger(CompactableUtils.class);

  public static Map<StoredTabletFile,Pair<Key,Key>> getFirstAndLastKeys(Tablet tablet,
      Set<StoredTabletFile> allFiles) throws IOException {
    final Map<StoredTabletFile,Pair<Key,Key>> result = new HashMap<>();
    final FileOperations fileFactory = FileOperations.getInstance();
    final VolumeManager fs = tablet.getTabletServer().getFileSystem();
    for (StoredTabletFile file : allFiles) {
      FileSystem ns = fs.getFileSystemByPath(file.getPath());
      try (FileSKVIterator openReader = fileFactory.newReaderBuilder()
          .forFile(file.getPathStr(), ns, ns.getConf(), tablet.getContext().getCryptoService())
          .withTableConfiguration(tablet.getTableConfiguration()).seekToBeginning().build()) {
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
        MajorCompactionReason.from(kind), tablet.getTabletServer().getFileSystem(),
        tablet.getTableConfiguration(), sc, ic, fileLenCache, tablet.getContext());

    request.setFiles(datafiles);

    // TODO switch compaction strat from using StoredTabletFile
    CompactionStrategy strategy = CompactableUtils.newInstance(tablet.getTableConfiguration(),
        csc.getClassName(), CompactionStrategy.class);
    strategy.init(csc.getOptions());

    try {
      if (strategy.shouldCompact(request)) {
        strategy.gatherInformation(request);
        var plan = strategy.getCompactionPlan(request);

        log.debug("Selected files using compaction strategy {} {} {} {}",
            strategy.getClass().getSimpleName(), csc.getOptions(), plan.inputFiles,
            plan.deleteFiles);

        plan.validate(datafiles.keySet());

        return plan;
      }
    } catch (IOException e) {
      // TODO ensure this is handled well
      throw new UncheckedIOException(e);
    }
    return new CompactionPlan();
  }

  static AccumuloConfiguration createCompactionConfiguration(AccumuloConfiguration base,
      WriteParameters p) {
    if (p == null)
      return base;

    ConfigurationCopy result = new ConfigurationCopy(base);
    if (p.getHdfsBlockSize() > 0) {
      result.set(Property.TABLE_FILE_BLOCK_SIZE, "" + p.getHdfsBlockSize());
    }
    if (p.getBlockSize() > 0) {
      result.set(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE, "" + p.getBlockSize());
    }
    if (p.getIndexBlockSize() > 0) {
      result.set(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE_INDEX, "" + p.getIndexBlockSize());
    }
    if (p.getCompressType() != null) {
      result.set(Property.TABLE_FILE_COMPRESSION_TYPE, p.getCompressType());
    }
    if (p.getReplication() != 0) {
      result.set(Property.TABLE_FILE_REPLICATION, "" + p.getReplication());
    }
    return result;
  }

  static AccumuloConfiguration createCompactionConfiguration(Tablet tablet,
      Set<CompactableFile> files) {
    var tconf = tablet.getTableConfiguration();

    var configurorClass = tconf.get(Property.TABLE_COMPACTION_CONFIGUROR);
    if (configurorClass == null || configurorClass.isBlank()) {
      return tconf;
    }

    var opts = getOptsMap(tconf, Property.TABLE_COMPACTION_CONFIGUROR_OPTS);

    return createCompactionConfiguration(tablet, files,
        new CompactionConfigurerConfig(configurorClass).setOptions(opts));
  }

  static AccumuloConfiguration createCompactionConfiguration(Tablet tablet,
      Set<CompactableFile> files, CompactionConfigurerConfig cfg) {
    CompactionConfigurer configurer = CompactableUtils.newInstance(tablet.getTableConfiguration(),
        cfg.getClassName(), CompactionConfigurer.class);

    configurer.init(new CompactionConfigurer.InitParamaters() {
      @Override
      public Map<String,String> getOptions() {
        return cfg.getOptions();
      }

      @Override
      public PluginEnvironment getEnvironment() {
        return new ServiceEnvironmentImpl(tablet.getContext());
      }

      @Override
      public TableId getTableId() {
        return tablet.getExtent().getTableId();
      }
    });

    var overrides = configurer.override(new CompactionConfigurer.InputParameters() {
      @Override
      public Collection<CompactableFile> getInputFiles() {
        return files;
      }

      @Override
      public PluginEnvironment getEnvironment() {
        return new ServiceEnvironmentImpl(tablet.getContext());
      }

      @Override
      public TableId getTableId() {
        return tablet.getExtent().getTableId();
      }
    });

    if (overrides.getOverrides().isEmpty()) {
      return tablet.getTableConfiguration();
    }

    ConfigurationCopy result = new ConfigurationCopy(tablet.getTableConfiguration());
    overrides.getOverrides().forEach(result::set);
    return result;
  }

  static <T> T newInstance(AccumuloConfiguration tableConfig, String className,
      Class<T> baseClass) {
    String context = tableConfig.get(Property.TABLE_CLASSPATH);
    try {
      return ConfigurationTypeHelper.getClassInstance(context, className, baseClass);
    } catch (IOException | ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }

  static Set<StoredTabletFile> selectFiles(Tablet tablet,
      SortedMap<StoredTabletFile,DataFileValue> datafiles,
      CompactionSelectorConfig selectorConfig) {

    // TODO how are exceptions handled
    CompactionSelector selector = newInstance(tablet.getTableConfiguration(),
        selectorConfig.getClassName(), CompactionSelector.class);
    selector.init(new CompactionSelector.InitParamaters() {

      @Override
      public Map<String,String> getOptions() {
        return selectorConfig.getOptions();
      }

      @Override
      public PluginEnvironment getEnvironment() {
        return new ServiceEnvironmentImpl(tablet.getContext());
      }
    });

    Selection selection = selector.select(new CompactionSelector.SelectionParameters() {

      @Override
      public PluginEnvironment getEnvironment() {
        return new ServiceEnvironmentImpl(tablet.getContext());
      }

      @Override
      public Collection<CompactableFile> getAvailableFiles() {
        // TODO Auto-generated method stub
        return Collections2.transform(datafiles.entrySet(),
            e -> new CompactableFileImpl(e.getKey(), e.getValue()));
      }

      @Override
      public Collection<Summary> getSummaries(Collection<CompactableFile> files,
          Predicate<SummarizerConfiguration> summarySelector) {

        var context = tablet.getContext();
        var tsrm = tablet.getTabletResources().getTabletServerResourceManager();

        SummaryCollection sc = new SummaryCollection();
        SummarizerFactory factory = new SummarizerFactory(tablet.getTableConfiguration());
        for (CompactableFile cf : files) {
          var file = CompactableFileImpl.toStoredTabletFile(cf);
          FileSystem fs = context.getVolumeManager().getFileSystemByPath(file.getPath());
          Configuration conf = context.getHadoopConf();
          SummaryCollection fsc = SummaryReader
              .load(fs, conf, factory, file.getPath(), summarySelector, tsrm.getSummaryCache(),
                  tsrm.getIndexCache(), tsrm.getFileLenCache(), context.getCryptoService())
              .getSummaries(Collections.singletonList(new Gatherer.RowRange(tablet.getExtent())));
          sc.merge(fsc, factory);
        }

        return sc.getSummaries();
      }
    });

    return selection.getFilesToCompact().stream().map(CompactableFileImpl::toStoredTabletFile)
        .collect(Collectors.toSet());
  }

  static Map<String,String> getOptsMap(AccumuloConfiguration conf, Property prefix) {
    var builder = ImmutableMap.<String,String>builder();
    conf.getAllPropertiesWithPrefix(prefix).forEach((k, v) -> {
      String optKey = k.substring(prefix.getKey().length());
      builder.put(optKey, v);
    });

    return builder.build();
  }

  static Deriver<CompactionDispatcher> createDispatcher(Tablet tablet) {
    return tablet.getTableConfiguration().newDeriver(conf -> {
      CompactionDispatcher newDispatcher = Property.createTableInstanceFromPropertyName(conf,
          Property.TABLE_COMPACTION_DISPATCHER, CompactionDispatcher.class, null);

      Map<String,String> opts = getOptsMap(conf, Property.TABLE_COMPACTION_DISPATCHER_OPTS);

      newDispatcher.init(new CompactionDispatcher.InitParameters() {
        @Override
        public TableId getTableId() {
          return tablet.getExtent().getTableId();
        }

        @Override
        public Map<String,String> getOptions() {
          return opts;
        }

        @Override
        public ServiceEnvironment getServiceEnv() {
          return new ServiceEnvironmentImpl(tablet.getContext());
        }
      });

      return newDispatcher;

    });
  }

  private static final class TableCompactionHelper implements CompactionHelper {
    private final CompactionSelectorConfig cselCfg2;
    private final CompactionStrategyConfig stratCfg2;
    private final Tablet tablet;
    private WriteParameters wp;
    private Set<StoredTabletFile> filesToDrop;

    private TableCompactionHelper(CompactionSelectorConfig cselCfg2,
        CompactionStrategyConfig stratCfg2, Tablet tablet) {
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
    public AccumuloConfiguration override(AccumuloConfiguration conf, Set<CompactableFile> files) {
      if (wp != null) {
        return createCompactionConfiguration(conf, wp);
      }

      return null;
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

      if (!UserCompactionUtils.isDefault(compactionConfig.getCompactionStrategy())) {
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
        // TODO seems like this should be set after the metadata update.. was before in the
        // exisitng
        // code
        tablet.setLastCompactionID(compactionId);

        MetadataTableUtil.updateTabletCompactID(tablet.getExtent(), compactionId,
            tablet.getTabletServer().getContext(), tablet.getTabletServer().getLock());
      }

      return selectedFiles;
    }

    @Override
    public AccumuloConfiguration override(AccumuloConfiguration conf, Set<CompactableFile> files) {
      if (!UserCompactionUtils.isDefault(compactionConfig.getConfigurer())) {
        return createCompactionConfiguration(tablet, files, compactionConfig.getConfigurer());
      } else if (!UserCompactionUtils.isDefault(compactionConfig.getCompactionStrategy())
          && wp != null) {
        return createCompactionConfiguration(conf, wp);
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

      CompactionSelectorConfig cselCfg = null;

      if (selectorClassName != null && !selectorClassName.isBlank()) {
        var opts = getOptsMap(tconf, Property.TABLE_COMPACTION_SELECTOR_OPTS);
        cselCfg = new CompactionSelectorConfig(selectorClassName).setOptions(opts);
      }

      CompactionStrategyConfig stratCfg = null;

      if (cselCfg == null && tconf.isPropertySet(Property.TABLE_COMPACTION_STRATEGY, true)) {
        // TODO log warning
        var stratClassName = tconf.get(Property.TABLE_COMPACTION_STRATEGY);
        var opts = getOptsMap(tconf, Property.TABLE_COMPACTION_STRATEGY_PREFIX);

        stratCfg = new CompactionStrategyConfig(stratClassName).setOptions(opts);
      }

      if (cselCfg != null || stratCfg != null) {
        return new TableCompactionHelper(cselCfg, stratCfg, tablet);
      }
    }

    // TODO Auto-generated method stub
    return null;
  }

  public static AccumuloConfiguration getCompactionConfig(CompactionKind kind, Tablet tablet,
      CompactionHelper driver, Set<CompactableFile> files) {
    if (kind == CompactionKind.USER || kind == CompactionKind.SELECTOR) {
      var oconf = driver.override(tablet.getTableConfiguration(), files);
      if (oconf != null)
        return oconf;
    }

    return createCompactionConfiguration(tablet, files);
  }

  static StoredTabletFile compact(Tablet tablet, CompactionJob job, Set<StoredTabletFile> jobFiles,
      Long compactionId, boolean propogateDeletesForSelected,
      CompactableImpl.CompactionHelper helper, List<IteratorSetting> iters)
      throws IOException, CompactionCanceledException {
    StoredTabletFile metaFile;
    CompactionEnv cenv = new CompactionEnv() {
      @Override
      public boolean isCompactionEnabled() {
        // TODO check for service change????
        return !tablet.isClosing();
      }

      @Override
      public IteratorScope getIteratorScope() {
        return IteratorScope.majc;
      }

      @Override
      public RateLimiter getReadLimiter() {
        return tablet.getTabletServer().getMajorCompactionReadLimiter();
      }

      @Override
      public RateLimiter getWriteLimiter() {
        return tablet.getTabletServer().getMajorCompactionWriteLimiter();
      }
    };

    int reason = MajorCompactionReason.from(job.getKind()).ordinal();

    AccumuloConfiguration tableConfig =
        getCompactionConfig(job.getKind(), tablet, helper, job.getFiles());

    SortedMap<StoredTabletFile,DataFileValue> allFiles = tablet.getDatafiles();
    HashMap<StoredTabletFile,DataFileValue> compactFiles = new HashMap<>();
    jobFiles.forEach(file -> compactFiles.put(file, allFiles.get(file)));

    boolean propogateDeletes = !allFiles.keySet().equals(compactFiles.keySet());

    if (job.getKind() == CompactionKind.USER || job.getKind() == CompactionKind.SELECTOR) {
      propogateDeletes = propogateDeletesForSelected;
    } else {
      // TODO if any compaction job contains all of the files at its time of creation, then should
      // probably not propogate
      // TODO this is done outside of sync block
      propogateDeletes = !allFiles.keySet().equals(compactFiles.keySet());
    }

    TabletFile newFile = tablet.getNextMapFilename(!propogateDeletes ? "A" : "C");
    TabletFile compactTmpName = new TabletFile(new Path(newFile.getMetaInsert() + "_tmp"));

    Compactor compactor = new Compactor(tablet.getContext(), tablet, compactFiles, null,
        compactTmpName, propogateDeletes, cenv, iters, reason, tableConfig);

    var mcs = compactor.call();

    if (job.getKind() == CompactionKind.USER || job.getKind() == CompactionKind.SELECTOR) {
      // TODO this approach will only drop files when there are files to compact... I think this is
      // what old code did
      helper.getFilesToDrop().forEach(f -> {
        if (allFiles.containsKey(f)) {
          compactFiles.put(f, allFiles.get(f));
        }
      });
    }

    metaFile = tablet.getDatafileManager().bringMajorCompactionOnline(compactFiles.keySet(),
        compactTmpName, newFile, compactionId,
        new DataFileValue(mcs.getFileSize(), mcs.getEntriesWritten()));
    return metaFile;
  }
}
