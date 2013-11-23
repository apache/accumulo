package org.apache.accumulo.core.client.mapreduce;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;

/**
 * AccumuloInputFormat which returns an "empty" RangeInputSplit
 */
public class EmptySplitsAccumuloInputFormat extends AccumuloInputFormat {
  
  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException {
    super.getSplits(context);
    
    return Arrays.<InputSplit> asList(new RangeInputSplit());
  }
}