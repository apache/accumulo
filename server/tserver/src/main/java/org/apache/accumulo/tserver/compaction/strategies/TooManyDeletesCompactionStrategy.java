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

import static org.apache.accumulo.core.client.summary.summarizers.DeletesSummarizer.DELETES_STAT;
import static org.apache.accumulo.core.client.summary.summarizers.DeletesSummarizer.TOTAL_STAT;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Predicate;

import org.apache.accumulo.core.client.admin.compaction.TooManyDeletesSelector;
import org.apache.accumulo.core.client.rfile.RFile.WriterOptions;
import org.apache.accumulo.core.client.summary.SummarizerConfiguration;
import org.apache.accumulo.core.client.summary.Summary;
import org.apache.accumulo.core.client.summary.summarizers.DeletesSummarizer;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.tserver.compaction.CompactionPlan;
import org.apache.accumulo.tserver.compaction.DefaultCompactionStrategy;
import org.apache.accumulo.tserver.compaction.MajorCompactionRequest;

/**
 * This compaction strategy works in concert with the {@link DeletesSummarizer}. Using the
 * statistics from DeleteSummarizer this strategy will compact all files in a table when the number
 * of deletes/non-deletes exceeds a threshold.
 *
 * <p>
 * This strategy has two options. First the {@value #THRESHOLD_OPT} option allows setting the point
 * at which a compaction will be triggered. This options defaults to {@value #THRESHOLD_OPT_DEFAULT}
 * and must be in the range (0.0, 1.0]. The second option is {@value #PROCEED_ZERO_NO_SUMMARY_OPT}
 * which determines if the strategy should proceed when a bulk imported file has no summary
 * information.
 *
 * <p>
 * If the delete summarizer was configured on a table that already had files, then those files will
 * have not summary information. This strategy can still proceed in this situation. It will fall
 * back to using Accumulo's estimated entries per file in this case. For the files without summary
 * information the estimated number of deletes will be zero. This fall back method will
 * underestimate deletes which will not lead to false positives, except for the case of bulk
 * imported files. Accumulo estimates that bulk imported files have zero entires. The second option
 * {@value #PROCEED_ZERO_NO_SUMMARY_OPT} determines if this strategy should proceed when it sees
 * bulk imported files that do not have summary data. This option defaults to
 * {@value #PROCEED_ZERO_NO_SUMMARY_OPT_DEFAULT}.
 *
 * <p>
 * Bulk files can be generated with summary information by calling
 * {@code AccumuloFileOutputFormat#setSummarizers(JobConf, SummarizerConfiguration...)} or
 * {@link WriterOptions#withSummarizers(SummarizerConfiguration...)}
 *
 * <p>
 * When this strategy does not decide to compact based on the number of deletes, then it will defer
 * the decision to the {@link DefaultCompactionStrategy}.
 *
 * <p>
 * Configuring this compaction strategy for a table will cause it to always queue compactions, even
 * though it may not decide to compact. These queued compactions may show up on the Accumulo monitor
 * page. This is because summary data can not be read until after compaction is queued and dequeued.
 * When the compaction is dequeued it can then decide not to compact. See <a
 * href=https://issues.apache.org/jira/browse/ACCUMULO-4573>ACCUMULO-4573</a>
 *
 * @since 2.0.0
 * @deprecated since 2.1.0 use {@link TooManyDeletesSelector} instead
 */
// Eclipse might show @SuppressWarnings("removal") as unnecessary.
// Eclipse is wrong. See https://bugs.eclipse.org/bugs/show_bug.cgi?id=565271
@SuppressWarnings("removal")
@Deprecated(since = "2.1.0", forRemoval = true)
public class TooManyDeletesCompactionStrategy extends DefaultCompactionStrategy {

  private boolean shouldCompact = false;

  private double threshold;

  private boolean proceed_bns;

  /**
   * This option should be a floating point number between 1 and 0.
   */
  public static final String THRESHOLD_OPT = "threshold";

  /**
   * The default threshold.
   */
  public static final String THRESHOLD_OPT_DEFAULT = ".25";

  public static final String PROCEED_ZERO_NO_SUMMARY_OPT = "proceed_zero_no_summary";

  public static final String PROCEED_ZERO_NO_SUMMARY_OPT_DEFAULT = "false";

  @Override
  public void init(Map<String,String> options) {
    this.threshold = Double.parseDouble(options.getOrDefault(THRESHOLD_OPT, THRESHOLD_OPT_DEFAULT));
    if (threshold <= 0.0 || threshold > 1.0) {
      throw new IllegalArgumentException(
          "Threshold must be in range (0.0, 1.0], saw : " + threshold);
    }

    this.proceed_bns = Boolean.parseBoolean(
        options.getOrDefault(PROCEED_ZERO_NO_SUMMARY_OPT, PROCEED_ZERO_NO_SUMMARY_OPT_DEFAULT));
  }

  @Override
  public boolean shouldCompact(MajorCompactionRequest request) {
    Collection<SummarizerConfiguration> configuredSummarizers =
        SummarizerConfiguration.fromTableProperties(request.getTableProperties());

    // check if delete summarizer is configured for table
    if (configuredSummarizers.stream().map(SummarizerConfiguration::getClassName)
        .anyMatch(cn -> cn.equals(DeletesSummarizer.class.getName()))) {
      // This is called before gatherInformation, so need to always queue for compaction until
      // context
      // can be gathered. Also its not safe to request summary
      // information here as its a blocking operation. Blocking operations are not allowed in
      // shouldCompact.
      return true;
    } else {
      return super.shouldCompact(request);
    }
  }

  @Override
  public void gatherInformation(MajorCompactionRequest request) throws IOException {
    super.gatherInformation(request);

    Predicate<SummarizerConfiguration> summarizerPredicate =
        conf -> conf.getClassName().equals(DeletesSummarizer.class.getName())
            && conf.getOptions().isEmpty();

    long total = 0;
    long deletes = 0;

    for (Entry<StoredTabletFile,DataFileValue> entry : request.getFiles().entrySet()) {
      Collection<Summary> summaries =
          request.getSummaries(Collections.singleton(entry.getKey()), summarizerPredicate);
      if (summaries.size() == 1) {
        Summary summary = summaries.iterator().next();
        total += summary.getStatistics().get(TOTAL_STAT);
        deletes += summary.getStatistics().get(DELETES_STAT);
      } else {
        long numEntries = entry.getValue().getNumEntries();
        if (numEntries == 0 && !proceed_bns) {
          shouldCompact = false;
          return;
        } else {
          // no summary data so use Accumulo's estimate of total entries in file
          total += entry.getValue().getNumEntries();
        }
      }
    }

    long nonDeletes = total - deletes;

    if (nonDeletes >= 0) {
      // check nonDeletes >= 0 because if this is not true then its clear evidence that the
      // estimates are off

      double ratio = deletes / (double) nonDeletes;
      shouldCompact = ratio >= threshold;
    } else {
      shouldCompact = false;
    }
  }

  @Override
  public CompactionPlan getCompactionPlan(MajorCompactionRequest request) {
    if (shouldCompact) {
      CompactionPlan cp = new CompactionPlan();
      cp.inputFiles.addAll(request.getFiles().keySet());
      return cp;
    }

    // fall back to default
    return super.getCompactionPlan(request);
  }
}
