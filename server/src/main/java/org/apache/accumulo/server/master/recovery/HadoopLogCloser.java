package org.apache.accumulo.server.master.recovery;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.log4j.Logger;

public class HadoopLogCloser implements LogCloser {
  
  private static Logger log = Logger.getLogger(HadoopLogCloser.class);

  @Override
  public long close(FileSystem fs, Path source) throws IOException {
    
    if (fs instanceof DistributedFileSystem) {
      DistributedFileSystem dfs = (DistributedFileSystem) fs;
      try {
        if (!dfs.recoverLease(source)) {
          log.info("Waiting for file to be closed " + source.toString());
          return 1000;
        }
        log.info("Recovered lease on " + source.toString());
        return 0;
      } catch (IOException ex) {
        log.warn("Error recovery lease on " + source.toString(), ex);
      }
    } else if (fs instanceof LocalFileSystem) {
      // ignore
    } else {
      throw new IllegalStateException("Don't know how to recover a lease for " + fs.getClass().getName());
    }
    fs.append(source).close();
    log.info("Recovered lease on " + source.toString() + " using append");
    return 0;
  }
  
}
