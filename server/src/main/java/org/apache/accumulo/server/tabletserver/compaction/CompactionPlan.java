package org.apache.accumulo.server.tabletserver.compaction;

import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.server.fs.FileRef;

/**
 * A plan for a compaction: the passes to run over input files, producing output files to replace them,  
 * the files that are *not* inputs to a compaction that should simply be deleted, and weather or not to 
 * propagate deletes from input files to output files.
 */
public class CompactionPlan {
  public List<CompactionPass> passes = new ArrayList<CompactionPass>();
  public List<FileRef> deleteFiles = new ArrayList<FileRef>();
  public boolean propogateDeletes = true;
  public String toString() {
    StringBuilder b = new StringBuilder();
    b.append(passes.toString());
    if (!deleteFiles.isEmpty()) { 
      b.append(" files to be deleted ");
      b.append(deleteFiles);
    }
    b.append(" propogateDeletes ");
    b.append(propogateDeletes);
    return b.toString(); 
  }
}
