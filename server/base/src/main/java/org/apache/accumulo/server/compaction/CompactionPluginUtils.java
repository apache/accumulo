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
package org.apache.accumulo.server.compaction;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.accumulo.core.classloader.ClassLoaderUtil;
import org.apache.accumulo.core.client.PluginEnvironment;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.PluginConfig;
import org.apache.accumulo.core.client.admin.compaction.CompactableFile;
import org.apache.accumulo.core.client.admin.compaction.CompactionConfigurer;
import org.apache.accumulo.core.client.admin.compaction.CompactionSelector;
import org.apache.accumulo.core.client.rfile.RFileSource;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.client.summary.SummarizerConfiguration;
import org.apache.accumulo.core.client.summary.Summary;
import org.apache.accumulo.core.clientImpl.UserCompactionUtils;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.TabletIdImpl;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.metadata.CompactableFileImpl;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.sample.impl.SamplerConfigurationImpl;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.accumulo.core.spi.compaction.CompactionDispatcher;
import org.apache.accumulo.core.summary.Gatherer;
import org.apache.accumulo.core.summary.SummarizerFactory;
import org.apache.accumulo.core.summary.SummaryCollection;
import org.apache.accumulo.core.summary.SummaryReader;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.ServiceEnvironmentImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Collections2;

public class CompactionPluginUtils {

  private static final Logger log = LoggerFactory.getLogger(CompactionPluginUtils.class);

  private static <T> T newInstance(AccumuloConfiguration tableConfig, String className,
      Class<T> baseClass) {
    String context = ClassLoaderUtil.tableContext(tableConfig);
    try {
      return ConfigurationTypeHelper.getClassInstance(context, className, baseClass);
    } catch (ReflectiveOperationException e) {
      throw new IllegalArgumentException(e);
    }
  }

  public static Set<StoredTabletFile> selectFiles(ServerContext context, KeyExtent extent,
      CompactionConfig compactionConfig, Map<StoredTabletFile,DataFileValue> allFiles) {
    if (!UserCompactionUtils.isDefault(compactionConfig.getSelector())) {
      return selectFiles(context, extent, allFiles, compactionConfig.getSelector());
    } else {
      return allFiles.keySet();
    }
  }

  private static Set<StoredTabletFile> selectFiles(ServerContext context, KeyExtent extent,
      Map<StoredTabletFile,DataFileValue> datafiles, PluginConfig selectorConfig) {

    log.debug("Selecting files for {} using {}", extent, selectorConfig);

    CompactionSelector selector = newInstance(context.getTableConfiguration(extent.tableId()),
        selectorConfig.getClassName(), CompactionSelector.class);

    final ServiceEnvironment senv = new ServiceEnvironmentImpl(context);

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
        return extent.tableId();
      }
    });

    CompactionSelector.Selection selection =
        selector.select(new CompactionSelector.SelectionParameters() {
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

            // ELASTICITY_TODO this may open files for user tables in the manager, need to avoid
            // this. See #3526

            try {
              var tableConf = context.getTableConfiguration(extent.tableId());

              SummaryCollection sc = new SummaryCollection();
              SummarizerFactory factory = new SummarizerFactory(tableConf);
              for (CompactableFile cf : files) {
                var file = CompactableFileImpl.toStoredTabletFile(cf);
                FileSystem fs = context.getVolumeManager().getFileSystemByPath(file.getPath());
                Configuration conf = context.getHadoopConf();
                RFileSource source = new RFileSource(new FSDataInputStream(fs.open(file.getPath())),
                    fs.getFileStatus(file.getPath()).getLen(), file.getRange());

                SummaryCollection fsc = SummaryReader
                    .load(conf, source, file.getFileName(), summarySelector, factory,
                        tableConf.getCryptoService())
                    .getSummaries(Collections.singletonList(new Gatherer.RowRange(extent)));

                sc.merge(fsc, factory);
              }
              return sc.getSummaries();
            } catch (IOException ioe) {
              throw new UncheckedIOException(ioe);
            }
          }

          @Override
          public TableId getTableId() {
            return extent.tableId();
          }

          @Override
          public TabletId getTabletId() {
            return new TabletIdImpl(extent);
          }

          @Override
          public Optional<SortedKeyValueIterator<Key,Value>> getSample(CompactableFile cf,
              SamplerConfiguration sc) {

            // ELASTICITY_TODO this may open files for user tables in the manager, need to avoid
            // this. See #3526

            try {
              var file = CompactableFileImpl.toStoredTabletFile(cf);
              FileSystem fs = context.getVolumeManager().getFileSystemByPath(file.getPath());
              Configuration conf = context.getHadoopConf();
              var tableConf = context.getTableConfiguration(extent.tableId());
              var iter = FileOperations.getInstance().newReaderBuilder()
                  .forFile(file, fs, conf, tableConf.getCryptoService())
                  .withTableConfiguration(tableConf).seekToBeginning().build();
              var sampleIter = iter.getSample(new SamplerConfigurationImpl(sc));
              if (sampleIter == null) {
                iter.close();
                return Optional.empty();
              }

              return Optional.of(sampleIter);
            } catch (IOException ioe) {
              throw new UncheckedIOException(ioe);
            }
          }
        });

    return selection.getFilesToCompact().stream().map(CompactableFileImpl::toStoredTabletFile)
        .collect(Collectors.toSet());
  }

  public static Map<String,String> computeOverrides(Optional<CompactionConfig> compactionConfig,
      ServerContext context, KeyExtent extent, Set<CompactableFile> inputFiles,
      Set<CompactableFile> selectedFiles) {

    if (compactionConfig.isPresent()
        && !UserCompactionUtils.isDefault(compactionConfig.orElseThrow().getConfigurer())) {
      return CompactionPluginUtils.computeOverrides(context, extent, inputFiles, selectedFiles,
          compactionConfig.orElseThrow().getConfigurer());
    }

    var tableConf = context.getTableConfiguration(extent.tableId());

    var configurorClass = tableConf.get(Property.TABLE_COMPACTION_CONFIGURER);
    if (configurorClass == null || configurorClass.isBlank()) {
      return Map.of();
    }

    var opts =
        tableConf.getAllPropertiesWithPrefixStripped(Property.TABLE_COMPACTION_CONFIGURER_OPTS);

    return CompactionPluginUtils.computeOverrides(context, extent, inputFiles, selectedFiles,
        new PluginConfig(configurorClass, opts));
  }

  public static Map<String,String> computeOverrides(ServerContext context, KeyExtent extent,
      Set<CompactableFile> inputFiles, Set<CompactableFile> selectedFiles, PluginConfig cfg) {

    CompactionConfigurer configurer = newInstance(context.getTableConfiguration(extent.tableId()),
        cfg.getClassName(), CompactionConfigurer.class);

    final ServiceEnvironment senv = new ServiceEnvironmentImpl(context);

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
        return extent.tableId();
      }
    });

    var overrides = configurer.override(new CompactionConfigurer.InputParameters() {
      @Override
      public Collection<CompactableFile> getInputFiles() {
        return inputFiles;
      }

      @Override
      public Set<CompactableFile> getSelectedFiles() {
        return selectedFiles;
      }

      @Override
      public PluginEnvironment getEnvironment() {
        return senv;
      }

      @Override
      public TableId getTableId() {
        return extent.tableId();
      }

      @Override
      public TabletId getTabletId() {
        return new TabletIdImpl(extent);
      }
    });

    if (overrides.getOverrides().isEmpty()) {
      return null;
    }

    return overrides.getOverrides();
  }

  static CompactionDispatcher createDispatcher(ServiceEnvironment env, TableId tableId) {

    var conf = env.getConfiguration(tableId);

    var className = conf.get(Property.TABLE_COMPACTION_DISPATCHER.getKey());

    Map<String,String> opts = new HashMap<>();

    conf.getWithPrefix(Property.TABLE_COMPACTION_DISPATCHER_OPTS.getKey()).forEach((k, v) -> {
      opts.put(k.substring(Property.TABLE_COMPACTION_DISPATCHER_OPTS.getKey().length()), v);
    });

    var finalOpts = Collections.unmodifiableMap(opts);

    CompactionDispatcher.InitParameters initParameters = new CompactionDispatcher.InitParameters() {
      @Override
      public Map<String,String> getOptions() {
        return finalOpts;
      }

      @Override
      public TableId getTableId() {
        return tableId;
      }

      @Override
      public ServiceEnvironment getServiceEnv() {
        return env;
      }
    };

    CompactionDispatcher dispatcher = null;
    try {
      dispatcher = env.instantiate(tableId, className, CompactionDispatcher.class);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }

    dispatcher.init(initParameters);

    return dispatcher;
  }
}
