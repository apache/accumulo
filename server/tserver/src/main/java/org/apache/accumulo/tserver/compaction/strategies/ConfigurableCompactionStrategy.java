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
package org.apache.accumulo.tserver.compaction.strategies;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.admin.compaction.CompactableFile;
import org.apache.accumulo.core.client.admin.compaction.CompactionConfigurer;
import org.apache.accumulo.core.client.admin.compaction.CompactionSelector;
import org.apache.accumulo.core.client.summary.SummarizerConfiguration;
import org.apache.accumulo.core.client.summary.Summary;
import org.apache.accumulo.core.compaction.CompactionSettings;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.sample.impl.SamplerConfigurationImpl;
import org.apache.hadoop.fs.Path;

/**
 * The compaction strategy used by the shell compact command.
 */
public class ConfigurableCompactionStrategy implements CompactionSelector, CompactionConfigurer {

  private abstract static class Test {
    abstract Set<CompactableFile> getFilesToCompact(SelectionParameters params);
  }

  private static class SummaryTest extends Test {

    private boolean selectExtraSummary;
    private boolean selectNoSummary;

    public SummaryTest(boolean selectExtraSummary, boolean selectNoSummary) {
      this.selectExtraSummary = selectExtraSummary;
      this.selectNoSummary = selectNoSummary;
    }

    @Override
    Set<CompactableFile> getFilesToCompact(SelectionParameters params) {

      Collection<SummarizerConfiguration> configs = SummarizerConfiguration
          .fromTableProperties(params.getEnvironment().getConfiguration(params.getTableId()));

      if (configs.isEmpty()) {
        return Set.of();
      } else {
        Set<CompactableFile> filesToCompact = new HashSet<>();
        Set<SummarizerConfiguration> configsSet = configs instanceof Set
            ? (Set<SummarizerConfiguration>) configs : new HashSet<>(configs);

        for (CompactableFile tabletFile : params.getAvailableFiles()) {
          Map<SummarizerConfiguration,Summary> sMap = new HashMap<>();
          Collection<Summary> summaries;
          summaries =
              params.getSummaries(Collections.singletonList(tabletFile), configsSet::contains);
          for (Summary summary : summaries) {
            sMap.put(summary.getSummarizerConfiguration(), summary);
          }

          for (SummarizerConfiguration sc : configs) {
            Summary summary = sMap.get(sc);

            if (summary == null && selectNoSummary) {
              filesToCompact.add(tabletFile);
              break;
            }

            if (summary != null && summary.getFileStatistics().getExtra() > 0
                && selectExtraSummary) {
              filesToCompact.add(tabletFile);
              break;
            }
          }
        }
        return filesToCompact;
      }
    }
  }

  private static class NoSampleTest extends Test {

    @Override
    Set<CompactableFile> getFilesToCompact(SelectionParameters params) {
      SamplerConfigurationImpl sc = SamplerConfigurationImpl.newSamplerConfig(
          new ConfigurationCopy(params.getEnvironment().getConfiguration(params.getTableId())));

      if (sc == null) {
        return Set.of();
      }

      Set<CompactableFile> filesToCompact = new HashSet<>();
      for (CompactableFile tabletFile : params.getAvailableFiles()) {
        Optional<SortedKeyValueIterator<Key,Value>> sample =
            params.getSample(tabletFile, sc.toSamplerConfiguration());

        if (sample.isEmpty()) {
          filesToCompact.add(tabletFile);
        }
      }

      return filesToCompact;
    }
  }

  private abstract static class FileSizeTest extends Test {
    private final long esize;

    private FileSizeTest(String s) {
      this.esize = Long.parseLong(s);
    }

    @Override
    Set<CompactableFile> getFilesToCompact(SelectionParameters params) {
      return params.getAvailableFiles().stream()
          .filter(cf -> shouldCompact(cf.getEstimatedSize(), esize)).collect(Collectors.toSet());
    }

    public abstract boolean shouldCompact(long fsize, long esize);
  }

  private abstract static class PatternPathTest extends Test {
    private Pattern pattern;

    private PatternPathTest(String p) {
      this.pattern = Pattern.compile(p);
    }

    @Override
    Set<CompactableFile> getFilesToCompact(SelectionParameters params) {
      return params.getAvailableFiles().stream()
          .filter(cf -> pattern.matcher(getInput(new Path(cf.getUri()))).matches())
          .collect(Collectors.toSet());
    }

    public abstract String getInput(Path path);

  }

  private List<Test> tests = new ArrayList<>();
  private boolean andTest = true;
  private int minFiles = 1;
  private Map<String,String> overrides = new HashMap<>();

  @Override
  public void init(
      org.apache.accumulo.core.client.admin.compaction.CompactionConfigurer.InitParameters iparams) {
    Set<Entry<String,String>> es = iparams.getOptions().entrySet();
    for (Entry<String,String> entry : es) {

      switch (CompactionSettings.valueOf(entry.getKey())) {
        case OUTPUT_COMPRESSION_OPT:
          overrides.put(Property.TABLE_FILE_COMPRESSION_TYPE.getKey(), entry.getValue());
          break;
        case OUTPUT_BLOCK_SIZE_OPT:
          overrides.put(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE.getKey(), entry.getValue());
          break;
        case OUTPUT_INDEX_BLOCK_SIZE_OPT:
          overrides.put(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE_INDEX.getKey(), entry.getValue());
          break;
        case OUTPUT_HDFS_BLOCK_SIZE_OPT:
          overrides.put(Property.TABLE_FILE_BLOCK_SIZE.getKey(), entry.getValue());
          break;
        case OUTPUT_REPLICATION_OPT:
          overrides.put(Property.TABLE_FILE_REPLICATION.getKey(), entry.getValue());
          break;
        default:
          throw new IllegalArgumentException("Unknown option " + entry.getKey());
      }
    }

  }

  @Override
  public Overrides override(InputParameters params) {
    return new Overrides(overrides);
  }

  @Override
  public void init(
      org.apache.accumulo.core.client.admin.compaction.CompactionSelector.InitParameters iparams) {
    boolean selectNoSummary = false;
    boolean selectExtraSummary = false;

    Set<Entry<String,String>> es = iparams.getOptions().entrySet();
    for (Entry<String,String> entry : es) {

      switch (CompactionSettings.valueOf(entry.getKey())) {
        case SF_EXTRA_SUMMARY:
          selectExtraSummary = true;
          break;
        case SF_NO_SUMMARY:
          selectNoSummary = true;
          break;
        case SF_NO_SAMPLE:
          tests.add(new NoSampleTest());
          break;
        case SF_LT_ESIZE_OPT:
          tests.add(new FileSizeTest(entry.getValue()) {
            @Override
            public boolean shouldCompact(long fsize, long esize) {
              return fsize < esize;
            }
          });
          break;
        case SF_GT_ESIZE_OPT:
          tests.add(new FileSizeTest(entry.getValue()) {
            @Override
            public boolean shouldCompact(long fsize, long esize) {
              return fsize > esize;
            }
          });
          break;
        case SF_NAME_RE_OPT:
          tests.add(new PatternPathTest(entry.getValue()) {
            @Override
            public String getInput(Path path) {
              return path.getName();
            }
          });
          break;
        case SF_PATH_RE_OPT:
          tests.add(new PatternPathTest(entry.getValue()) {
            @Override
            public String getInput(Path path) {
              return path.toString();
            }
          });
          break;
        case MIN_FILES_OPT:
          minFiles = Integer.parseInt(entry.getValue());
          break;
        default:
          throw new IllegalArgumentException("Unknown option " + entry.getKey());
      }
    }

    if (selectExtraSummary || selectNoSummary) {
      tests.add(new SummaryTest(selectExtraSummary, selectNoSummary));
    }
  }

  @Override
  public Selection select(SelectionParameters sparams) {

    Set<CompactableFile> filesToCompact =
        tests.isEmpty() ? new HashSet<>(sparams.getAvailableFiles()) : null;

    for (Test test : tests) {
      var files = test.getFilesToCompact(sparams);
      if (filesToCompact == null) {
        filesToCompact = files;
      } else if (andTest) {
        filesToCompact.retainAll(files);
      } else {
        filesToCompact.addAll(files);
      }
    }

    if (filesToCompact.size() < minFiles) {
      return new Selection(Set.of());
    }

    return new Selection(filesToCompact);
  }

}
