package org.apache.accumulo.core.client.summary;

import java.util.Map;

/**
 * This is a helper class that simplifies merging summaries. For the case when a summaryKey exist in both maps it ask the user to merge it. For the cases were a
 * summary key only exist in one map it copies it over.
 * 
 *
 */
// TODO better name?
public abstract class SimpleMergeSummarizer implements KeyValueSummarizer {

  public abstract long merge(String summaryKey, long v1, long v2);

  public void merge(Map<String,Long> summary1, Map<String,Long> summary2) {
    summary2.forEach((k2, v2) -> {
      Long v1 = summary1.get(k2);
      if (v1 == null) {
        summary1.put(k2, v2);
      } else {
        long v3 = merge(k2, v1, v2);
        summary1.put(k2, v3);
      }
    });
  }
}
