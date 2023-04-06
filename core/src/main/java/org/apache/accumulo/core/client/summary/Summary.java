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
package org.apache.accumulo.core.client.summary;

import java.util.Map;

/**
 * This class encapsulates summary statistics, information about how those statistics were
 * generated, and information about files the statistics were obtained from.
 *
 * @see Summarizer
 * @since 2.0.0
 */
public class Summary {

  /**
   * @since 2.0.0
   */
  public static class FileStatistics {
    private final long total;
    private final long missing;
    private final long extra;
    private final long large;
    private final long deleted;

    private FileStatistics(long total, long missing, long extra, long large, long deleted) {
      this.total = total;
      this.missing = missing;
      this.extra = extra;
      this.large = large;
      this.deleted = deleted;
    }

    /**
     * @return The total number of files from which summary information was obtained.
     */
    public long getTotal() {
      return total;
    }

    /**
     * @return The number of files that did not contain the requested summary information. When this
     *         is non-zero, it means that summary counts may be incomplete. In the Accumulo shell,
     *         the compact command has a -{@code -sf-no-summary} option to compact files missing
     *         summary information. The compaction will create the summary information. This could
     *         be done over a range of the table to avoid doing the entire table at once.
     */
    public long getMissing() {
      return missing;
    }

    /**
     * @return The number of files that had summary information outside of a tablet or query range
     *         boundaries. When this is non-zero, it means that summary counts may be artificially
     *         inflated or contain extraneous information. In the Accumulo shell, the compact
     *         command has a -{@code -sf-extra-summary} option to compact files with extra summary
     *         information.
     */
    public long getExtra() {
      return extra;
    }

    /**
     * @return The number of files that an attempt was made to generate summaries, but the
     *         summarizer generated a summary that was larger than the configured maximum. For these
     *         files no summary statistics are stored. Only the fact that summarization was
     *         attempted and failed is stored.
     * @see Summarizer.Collector#summarize(org.apache.accumulo.core.client.summary.Summarizer.StatisticConsumer)
     */
    public long getLarge() {
      return large;
    }

    /**
     * @return The number of files that were deleted after the summary retrieval operations started.
     *         This is a rare race condition where a compaction causes a file to be deleted while
     *         retrieving summaries. When this happens, the file that replaced the deleted file can
     *         not be used because it may contain duplication summary information for other files.
     *         Avoiding this race condition would be expensive, so reporting it was chosen. If this
     *         condition must be avoided, then compactions must be stopped. Compactions could be
     *         stopped on a cloned table to avoid this.
     */
    public long getDeleted() {
      return deleted;
    }

    /**
     * @return The total number of files that had some kind of issue which would cause summary
     *         statistics to be inaccurate. This is the sum of {@link #getMissing()},
     *         {@link #getExtra()}, {@link #getLarge()}, and {@link #getDeleted()}.
     */
    public long getInaccurate() {
      return getMissing() + getExtra() + getLarge() + getDeleted();
    }

    @Override
    public String toString() {
      return String.format("[total:%,d, missing:%,d, extra:%,d, large:%,d, deleted:%,d]", total,
          missing, extra, large, deleted);
    }
  }

  private final Map<String,Long> statistics;
  private final SummarizerConfiguration config;
  private final FileStatistics fileStats;

  public Summary(Map<String,Long> summary, SummarizerConfiguration config, long totalFiles,
      long filesMissingSummary, long filesWithExtra, long filesWithLarge, long deletedFiles) {
    this.statistics = Map.copyOf(summary);
    this.config = config;
    this.fileStats = new FileStatistics(totalFiles, filesMissingSummary, filesWithExtra,
        filesWithLarge, deletedFiles);
  }

  /**
   * @return Statistics about the files from which summary statistics were obtained.
   */
  public FileStatistics getFileStatistics() {
    return fileStats;
  }

  /**
   * @return The configuration used to generate and combine the summary statistics
   */
  public SummarizerConfiguration getSummarizerConfiguration() {
    return config;
  }

  /**
   * @return An immutable map of the statistics that were generated and merged by the specified
   *         {@link Summarizer}.
   */
  public Map<String,Long> getStatistics() {
    return statistics;
  }

  @Override
  public String toString() {
    return "config : " + config + " filestats : " + fileStats + " statistics : " + statistics;
  }
}
