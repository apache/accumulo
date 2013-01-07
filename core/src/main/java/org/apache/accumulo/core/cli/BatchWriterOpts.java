package org.apache.accumulo.core.cli;

import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.cli.ClientOpts.MemoryConverter;
import org.apache.accumulo.core.cli.ClientOpts.TimeConverter;
import org.apache.accumulo.core.client.BatchWriterConfig;

import com.beust.jcommander.Parameter;

public class BatchWriterOpts {
  private static final BatchWriterConfig BWDEFAULTS = new BatchWriterConfig();
  
  @Parameter(names="--batchThreads", description="Number of threads to use when writing large batches")
  public Integer batchThreads = BWDEFAULTS.getMaxWriteThreads();

  @Parameter(names="--batchLatency", converter=TimeConverter.class, description="The maximum time to wait before flushing data to servers when writing")
  public Long batchLatency = BWDEFAULTS.getMaxLatency(TimeUnit.MILLISECONDS);
  
  @Parameter(names="--batchMemory", converter=MemoryConverter.class, description="memory used to batch data when writing")
  public Long batchMemory = BWDEFAULTS.getMaxMemory();
  
  @Parameter(names="--batchTimeout", converter=TimeConverter.class, description="timeout used to fail a batch write")
  public Long batchTimeout = BWDEFAULTS.getTimeout(TimeUnit.MILLISECONDS);
  
  public BatchWriterConfig getBatchWriterConfig() {
    BatchWriterConfig config = new BatchWriterConfig();
    config.setMaxLatency(this.batchLatency, TimeUnit.MILLISECONDS);
    config.setMaxMemory(this.batchMemory);
    config.setTimeout(this.batchTimeout, TimeUnit.MILLISECONDS);
    return config;
  }
  
}
