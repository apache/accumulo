package org.apache.accumulo.core.client.mapreduce;

import java.io.IOException;
import java.util.List;

import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;

/**
 * AccumuloInputFormat which returns an "empty" RangeInputSplit
 */
public class BadPasswordSplitsAccumuloInputFormat extends AccumuloInputFormat {
  
  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException {
    List<InputSplit> splits = super.getSplits(context);
    
    for (InputSplit split : splits) {
      RangeInputSplit rangeSplit = (RangeInputSplit) split;
      rangeSplit.setToken(new PasswordToken("anythingelse"));
    }
    
    return splits;
  }
}