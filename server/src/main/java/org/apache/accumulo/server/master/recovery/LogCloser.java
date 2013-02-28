package org.apache.accumulo.server.master.recovery;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public interface LogCloser {
  public long close(FileSystem fs, Path path) throws IOException;
}
