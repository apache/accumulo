package org.apache.accumulo.server.tabletserver.compaction;

import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.server.fs.FileRef;

/**
 * Information about a single compaction pass: input files and the number of output files to write.
 * Presently, the number of output files must always be 1.
 */
public class CompactionPass {
  public List<FileRef> inputFiles = new ArrayList<FileRef>();
  public int outputFiles = 1;
  public String toString() {
    return inputFiles.toString() + " -> " + outputFiles + " files";
  }
}
