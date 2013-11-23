package org.apache.accumulo.core.client.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;

/**
 * AccumuloInputFormat which returns an "empty" RangeInputSplit
 */
public class EmptySplitsAccumuloInputFormat extends AccumuloInputFormat {
  
  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException {
    List<InputSplit> oldSplits = super.getSplits(context);
    List<InputSplit> newSplits = new ArrayList<InputSplit>(oldSplits.size());
    
    // Copy only the necessary information
    for (InputSplit oldSplit : oldSplits) {
      RangeInputSplit newSplit = new RangeInputSplit((RangeInputSplit) oldSplit);
      newSplits.add(newSplit);
    }
    
    
    return newSplits;
  }
}