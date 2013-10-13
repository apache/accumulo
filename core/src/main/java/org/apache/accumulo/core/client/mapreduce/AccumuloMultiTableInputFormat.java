package org.apache.accumulo.core.client.mapreduce;

import org.apache.accumulo.core.client.mapreduce.lib.util.InputConfigurator;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.format.DefaultFormatter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class AccumuloMultiTableInputFormat extends AbstractInputFormat<Key,Value>{
  
  /**
   * Sets the {@link BatchScanConfig} objects on the given Hadoop configuration
   * 
   * @param job
   *          the Hadoop job instance to be configured
   * @param configs
   *          the table query configs to be set on the configuration.
   * @since 1.6.0
   */
  public static void setBatchScanConfigs(Job job, BatchScanConfig... configs) {
    checkNotNull(configs);
    InputConfigurator.setTableQueryConfigs(CLASS, getConfiguration(job), configs);
  }
  
  @Override
  public RecordReader<Key, Value> createRecordReader(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
    log.setLevel(getLogLevel(context));
    return new RecordReaderBase<Key,Value>() {
      @Override
      public boolean nextKeyValue() throws IOException, InterruptedException {
        if (scannerIterator.hasNext()) {
          ++numKeysRead;
          Map.Entry<Key,Value> entry = scannerIterator.next();
          currentK = currentKey = entry.getKey();
          currentV = currentValue = entry.getValue();
          if (log.isTraceEnabled())
            log.trace("Processing key/value pair: " + DefaultFormatter.formatEntry(entry, true));
          return true;
        }
        return false;
      }
    };
  }
}
