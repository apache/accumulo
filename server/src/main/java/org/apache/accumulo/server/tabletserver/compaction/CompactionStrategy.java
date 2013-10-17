package org.apache.accumulo.server.tabletserver.compaction;

import java.io.IOException;
import java.util.List;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileSKVWriter;

/**
 * The interface for customizing major compactions.
 */
public interface CompactionStrategy {
  
  /**
   * Called for each output key/value to determine which file should get which key/value pair.
   */
  public interface Writer {
    void write(Key key, Value value, List<FileSKVWriter> outputFiles) throws IOException;
  }

  /**
   * Called prior to obtaining the tablet lock, useful for examining metadata or indexes.
   * @param request basic details about the tablet
   * @throws IOException
   */
  void gatherInformation(MajorCompactionRequest request) throws IOException;
  
  /** 
   * Get the plan for compacting a tablets files.  Called while holding the tablet lock, so it should not be doing any blocking.
   * @param request basic details about the tablet
   * @return the plan for a major compaction
   * @throws IOException
   */
  CompactionPlan getCompactionPlan(MajorCompactionRequest request) throws IOException;
  
  /**
   * Get the callback for this compaction to determine where to write the output.
   * @return
   */
  Writer getCompactionWriter();
  
}
