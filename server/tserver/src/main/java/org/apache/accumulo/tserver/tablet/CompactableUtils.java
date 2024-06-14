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
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.accumulo.core.classloader.ClassLoaderUtil;
import org.apache.accumulo.core.client.PluginEnvironment;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.PluginConfig;
import org.apache.accumulo.core.client.admin.compaction.CompactableFile;
import org.apache.accumulo.core.client.admin.compaction.CompactionConfigurer;
import org.apache.accumulo.core.client.admin.compaction.CompactionSelector;
import org.apache.accumulo.core.client.admin.compaction.CompactionSelector.Selection;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.client.summary.SummarizerConfiguration;
import org.apache.accumulo.core.client.summary.Summary;
import org.apache.accumulo.core.clientImpl.UserCompactionUtils;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.TabletIdImpl;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.metadata.CompactableFileImpl;
import org.apache.accumulo.core.metadata.ReferencedTabletFile;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.sample.impl.SamplerConfigurationImpl;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.core.summary.Gatherer;
import org.apache.accumulo.core.summary.SummarizerFactory;
import org.apache.accumulo.core.summary.SummaryCollection;
import org.apache.accumulo.core.summary.SummaryReader;
import org.apache.accumulo.core.util.compaction.DeprecatedCompactionKind;
import org.apache.accumulo.server.ServiceEnvironmentImpl;
import org.apache.accumulo.server.compaction.CompactionStats;
import org.apache.accumulo.server.compaction.FileCompactor;
import org.apache.accumulo.server.compaction.FileCompactor.CompactionCanceledException;
import org.apache.accumulo.server.compaction.FileCompactor.CompactionEnv;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.server.util.MetadataTableUtil;
import org.apache.accumulo.tserver.tablet.CompactableImpl.CompactionHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Collections2;

public class CompactableUtils {

  static Map<String,String> computeOverrides(Tablet tablet, Set<CompactableFile> inputFiles,
      Set<StoredTabletFile> selectedFiles, CompactionKind kind) {
    var tconf = tablet.getTableConfiguration();

    var configurorClass = tconf.get(Property.TABLE_COMPACTION_CONFIGURER);
    if (configurorClass == null || configurorClass.isBlank()) {
      return null;
    }

    var opts = tconf.getAllPropertiesWithPrefixStripped(Property.TABLE_COMPACTION_CONFIGURER_OPTS);

    return computeOverrides(tablet, inputFiles, selectedFiles,
        new PluginConfig(configurorClass, opts), kind);
  }

  static Map<String,String> computeOverrides(Tablet tablet, Set<CompactableFile> inputFiles,
      Set<StoredTabletFile> selectedFiles, PluginConfig cfg, CompactionKind kind) {
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
        return inputFiles;
      }

      @Override
      public Set<CompactableFile> getSelectedFiles() {
        if (kind == CompactionKind.USER || kind == DeprecatedCompactionKind.SELECTOR) {
          var dataFileSizes = tablet.getDatafileManager().getDatafileSizes();
          return selectedFiles.stream().map(f -> new CompactableFileImpl(f, dataFileSizes.get(f)))
              .collect(Collectors.toSet());
        } else { // kind == CompactionKind.SYSTEM
          return Collections.emptySet();
        }
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
    } catch (ReflectiveOperationException e) {
      throw new IllegalArgumentException(e);
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
              .forFile(ReferencedTabletFile.of(path), ns, ns.getConf(),
                  tableConf.getCryptoService())
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
    private final Tablet tablet;

    private TableCompactionHelper(PluginConfig cselCfg2, Tablet tablet) {
      this.cselCfg2 = Objects.requireNonNull(cselCfg2);
      this.tablet = Objects.requireNonNull(tablet);
    }

    @Override
    public Set<StoredTabletFile> selectFiles(SortedMap<StoredTabletFile,DataFileValue> allFiles) {
      return CompactableUtils.selectFiles(tablet, allFiles, cselCfg2);
    }

    @Override
    public Map<String,String> getConfigOverrides(Set<CompactableFile> inputFiles,
        Set<StoredTabletFile> selectedFiles, CompactionKind kind) {
      return null;
    }
  }

  private static final class UserCompactionHelper implements CompactionHelper {
    private final CompactionConfig compactionConfig;
    private final Tablet tablet;
    private final Long compactionId;

    private UserCompactionHelper(CompactionConfig compactionConfig, Tablet tablet,
        Long compactionId) {
      this.compactionConfig = compactionConfig;
      this.tablet = tablet;
      this.compactionId = compactionId;
    }

    @Override
    public Set<StoredTabletFile> selectFiles(SortedMap<StoredTabletFile,DataFileValue> allFiles) {

      final Set<StoredTabletFile> selectedFiles;

      if (!UserCompactionUtils.isDefault(compactionConfig.getSelector())) {
        selectedFiles =
            CompactableUtils.selectFiles(tablet, allFiles, compactionConfig.getSelector());
      } else {
        selectedFiles = allFiles.keySet();
      }

      if (selectedFiles.isEmpty()) {
        MetadataTableUtil.updateTabletCompactID(tablet.getExtent(), compactionId,
            tablet.getTabletServer().getContext(), tablet.getTabletServer().getLock());

        tablet.setLastCompactionID(compactionId);
      }

      return selectedFiles;
    }

    @Override
    public Map<String,String> getConfigOverrides(Set<CompactableFile> inputFiles,
        Set<StoredTabletFile> selectedFiles, CompactionKind kind) {
      if (!UserCompactionUtils.isDefault(compactionConfig.getConfigurer())) {
        return computeOverrides(tablet, inputFiles, selectedFiles, compactionConfig.getConfigurer(),
            kind);
      }

      return null;
    }
  }

  @SuppressWarnings("deprecation")
  private static final Property SELECTOR_PROP = Property.TABLE_COMPACTION_SELECTOR;
  @SuppressWarnings("deprecation")
  private static final Property SELECTOR_OPTS_PROP = Property.TABLE_COMPACTION_SELECTOR_OPTS;

  public static CompactionHelper getHelper(CompactionKind kind, Tablet tablet, Long compactionId,
      CompactionConfig compactionConfig) {
    if (kind == CompactionKind.USER) {
      return new UserCompactionHelper(compactionConfig, tablet, compactionId);
    } else if (kind == DeprecatedCompactionKind.SELECTOR) {
      var tconf = tablet.getTableConfiguration();
      var selectorClassName = tconf.get(SELECTOR_PROP);

      PluginConfig cselCfg = null;

      if (selectorClassName != null && !selectorClassName.isBlank()) {
        var opts = tconf.getAllPropertiesWithPrefixStripped(SELECTOR_OPTS_PROP);
        cselCfg = new PluginConfig(selectorClassName, opts);
      }

      if (cselCfg != null) {
        return new TableCompactionHelper(cselCfg, tablet);
      }
    }

    return null;
  }

  public static Map<String,String> getOverrides(CompactionKind kind, Tablet tablet,
      CompactionHelper driver, Set<CompactableFile> inputFiles,
      Set<StoredTabletFile> selectedFiles) {

    Map<String,String> overrides = null;

    if (kind == CompactionKind.USER || kind == DeprecatedCompactionKind.SELECTOR) {
      overrides = driver.getConfigOverrides(inputFiles, selectedFiles, kind);
    }

    if (overrides == null) {
      overrides = computeOverrides(tablet, inputFiles, selectedFiles, kind);
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
      Map<StoredTabletFile,DataFileValue> compactFiles, ReferencedTabletFile tmpFileName)
      throws IOException, CompactionCanceledException {
    TableConfiguration tableConf = tablet.getTableConfiguration();

    AccumuloConfiguration compactionConfig =
        getCompactionConfig(tableConf, getOverrides(job.getKind(), tablet, cInfo.localHelper,
            job.getFiles(), cInfo.selectedFiles));

    final FileCompactor compactor = new FileCompactor(tablet.getContext(), tablet.getExtent(),
        compactFiles, tmpFileName, cInfo.propagateDeletes, cenv, cInfo.iters, compactionConfig,
        tableConf.getCryptoService(), tablet.getPausedCompactionMetrics());

    final Runnable compactionCancellerTask = () -> {
      if (!cenv.isCompactionEnabled()) {
        compactor.interrupt();
      }
    };
    final ScheduledFuture<?> future = tablet.getContext().getScheduledExecutor()
        .scheduleWithFixedDelay(compactionCancellerTask, 10, 10, TimeUnit.SECONDS);
    try {
      return compactor.call();
    } finally {
      future.cancel(true);
    }
  }

  /**
   * Finish major compaction by bringing the new file online and returning the completed file.
   */
  static Optional<StoredTabletFile> bringOnline(DatafileManager datafileManager,
      CompactableImpl.CompactionInfo cInfo, CompactionStats stats,
      Map<StoredTabletFile,DataFileValue> compactFiles, ReferencedTabletFile compactTmpName)
      throws IOException {

    var dfv = new DataFileValue(stats.getFileSize(), stats.getEntriesWritten());
    return datafileManager.bringMajorCompactionOnline(compactFiles.keySet(), compactTmpName,
        cInfo.checkCompactionId, cInfo.selectedFiles, dfv, Optional.empty());
  }

  public static ReferencedTabletFile computeCompactionFileDest(ReferencedTabletFile tmpFile) {
    String newFilePath = tmpFile.getNormalizedPathStr();
    int idx = newFilePath.indexOf("_tmp");
    if (idx > 0) {
      newFilePath = newFilePath.substring(0, idx);
    } else {
      throw new IllegalArgumentException("Expected compaction tmp file "
          + tmpFile.getNormalizedPathStr() + " to have suffix '_tmp'");
    }
    return new ReferencedTabletFile(new Path(newFilePath));
  }

}
