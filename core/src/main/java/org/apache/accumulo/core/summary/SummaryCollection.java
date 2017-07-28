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

package org.apache.accumulo.core.summary;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.summary.Summarizer;
import org.apache.accumulo.core.client.summary.SummarizerConfiguration;
import org.apache.accumulo.core.client.summary.Summary;
import org.apache.accumulo.core.data.thrift.TSummaries;
import org.apache.accumulo.core.data.thrift.TSummarizerConfiguration;
import org.apache.accumulo.core.data.thrift.TSummary;

import com.google.common.base.Preconditions;

/**
 * This class facilitates merging, storing, and serializing (to/from thrift) intermediate summary information.
 */
public class SummaryCollection {

  private static class MergedSummary {
    Map<String,Long> summary;
    long filesContaining;
    long filesExceedingBoundry;
    long filesLarge;

    public MergedSummary(FileSummary entry) {
      this.summary = entry.summary;
      this.filesContaining = 1;
      this.filesExceedingBoundry = entry.exceededBoundry ? 1 : 0;
      this.filesLarge = entry.exceededMaxSize ? 1 : 0;
    }

    public MergedSummary(TSummary tSummary) {
      this.summary = new HashMap<>(tSummary.getSummary());
      this.filesContaining = tSummary.getFilesContaining();
      this.filesExceedingBoundry = tSummary.getFilesExceeding();
      this.filesLarge = tSummary.getFilesLarge();
    }

    public void merge(MergedSummary other, SummarizerConfiguration config, SummarizerFactory factory) {

      if (summary == null && other.summary != null) {
        summary = new HashMap<>(other.summary);
      } else if (summary != null && other.summary != null) {
        Summarizer summarizer = factory.getSummarizer(config);
        summarizer.combiner(config).merge(summary, other.summary);
      }

      filesContaining += other.filesContaining;
      filesExceedingBoundry += other.filesExceedingBoundry;
      filesLarge += other.filesLarge;
    }

    public TSummary toThrift(SummarizerConfiguration key) {
      TSummarizerConfiguration tsumConf = SummarizerConfigurationUtil.toThrift(key);
      return new TSummary(summary, tsumConf, filesContaining, filesExceedingBoundry, filesLarge);
    }

  }

  private Map<SummarizerConfiguration,MergedSummary> mergedSummaries;
  private long totalFiles;
  private long deletedFiles;

  public SummaryCollection() {
    mergedSummaries = new HashMap<>();
    totalFiles = 0;
  }

  public SummaryCollection(TSummaries tsums) {
    mergedSummaries = new HashMap<>();
    for (TSummary tSummary : tsums.getSummaries()) {
      SummarizerConfiguration sconf = SummarizerConfigurationUtil.fromThrift(tSummary.getConfig());
      mergedSummaries.put(sconf, new MergedSummary(tSummary));
    }

    totalFiles = tsums.getTotalFiles();
    deletedFiles = tsums.getDeletedFiles();
  }

  SummaryCollection(Collection<FileSummary> initialEntries) {
    this(initialEntries, false);
  }

  SummaryCollection(Collection<FileSummary> initialEntries, boolean deleted) {
    if (deleted) {
      Preconditions.checkArgument(initialEntries.size() == 0);
    }
    mergedSummaries = new HashMap<>();
    for (FileSummary entry : initialEntries) {
      mergedSummaries.put(entry.conf, new MergedSummary(entry));
    }
    totalFiles = 1;
    this.deletedFiles = deleted ? 1 : 0;
  }

  static class FileSummary {

    private SummarizerConfiguration conf;
    private Map<String,Long> summary;
    private boolean exceededBoundry;
    private boolean exceededMaxSize;

    FileSummary(SummarizerConfiguration conf, Map<String,Long> summary, boolean exceededBoundry) {
      this.conf = conf;
      this.summary = summary;
      this.exceededBoundry = exceededBoundry;
      this.exceededMaxSize = false;
    }

    FileSummary(SummarizerConfiguration conf) {
      this.conf = conf;
      this.summary = new HashMap<>();
      this.exceededBoundry = false;
      this.exceededMaxSize = true;
    }
  }

  public void merge(SummaryCollection other, SummarizerFactory factory) {
    for (Entry<SummarizerConfiguration,MergedSummary> entry : other.mergedSummaries.entrySet()) {
      MergedSummary ms = mergedSummaries.get(entry.getKey());
      if (ms == null) {
        mergedSummaries.put(entry.getKey(), entry.getValue());
      } else {
        ms.merge(entry.getValue(), entry.getKey(), factory);
      }
    }

    this.totalFiles += other.totalFiles;
    this.deletedFiles += other.deletedFiles;
  }

  public static SummaryCollection merge(SummaryCollection sc1, SummaryCollection sc2, SummarizerFactory factory) {
    SummaryCollection ret = new SummaryCollection();
    ret.merge(sc1, factory);
    ret.merge(sc2, factory);
    return ret;
  }

  public List<Summary> getSummaries() {
    ArrayList<Summary> ret = new ArrayList<>(mergedSummaries.size());

    for (Entry<SummarizerConfiguration,MergedSummary> entry : mergedSummaries.entrySet()) {
      SummarizerConfiguration config = entry.getKey();
      MergedSummary ms = entry.getValue();

      ret.add(new Summary(ms.summary, config, totalFiles, (totalFiles - deletedFiles) - ms.filesContaining, ms.filesExceedingBoundry, ms.filesLarge,
          deletedFiles));
    }

    return ret;
  }

  public long getTotalFiles() {
    return totalFiles;
  }

  public TSummaries toThrift() {
    List<TSummary> summaries = new ArrayList<>(mergedSummaries.size());
    for (Entry<SummarizerConfiguration,MergedSummary> entry : mergedSummaries.entrySet()) {
      summaries.add(entry.getValue().toThrift(entry.getKey()));
    }

    return new TSummaries(true, -1l, totalFiles, deletedFiles, summaries);
  }
}
