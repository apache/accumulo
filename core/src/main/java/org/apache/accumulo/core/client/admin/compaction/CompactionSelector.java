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
package org.apache.accumulo.core.client.admin.compaction;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

import org.apache.accumulo.core.client.PluginEnvironment;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.client.summary.SummarizerConfiguration;
import org.apache.accumulo.core.client.summary.Summary;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

/**
 * This class selects which files a user compaction will compact. It can also be configured per
 * table to periodically select files to compact.
 *
 * @since 2.1.0
 */
public interface CompactionSelector {

  public interface InitParameters {
    Map<String,String> getOptions();

    TableId getTableId();

    PluginEnvironment getEnvironment();
  }

  void init(InitParameters iparams);

  public interface SelectionParameters {
    PluginEnvironment getEnvironment();

    Collection<CompactableFile> getAvailableFiles();

    Collection<Summary> getSummaries(Collection<CompactableFile> files,
        Predicate<SummarizerConfiguration> summarySelector);

    TableId getTableId();

    /**
     * @return the tablet id of the tablet being compacted
     * @since 3.0.0
     */
    TabletId getTabletId();

    Optional<SortedKeyValueIterator<Key,Value>> getSample(CompactableFile cf,
        SamplerConfiguration sc);

  }

  public static class Selection {
    private final Collection<CompactableFile> filesToCompact;

    public Selection(Collection<CompactableFile> filesToCompact) {
      this.filesToCompact = Set.copyOf(filesToCompact);
    }

    public Collection<CompactableFile> getFilesToCompact() {
      return filesToCompact;
    }
  }

  Selection select(SelectionParameters sparams);
}
