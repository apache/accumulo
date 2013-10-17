package org.apache.accumulo.server.tabletserver.compaction;

import java.io.IOException;
import java.util.List;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileSKVWriter;

public class DefaultWriter implements CompactionStrategy.Writer {
  
  @Override
  public void write(Key key, Value value, List<FileSKVWriter> outputFiles) throws IOException {
    outputFiles.get(0).append(key, value);
  }
  
}
