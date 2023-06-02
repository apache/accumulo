package org.apache.accumulo.core.metadata;

import org.apache.hadoop.fs.Path;

public interface TabletFile<T extends TabletFile<T>> {

  /**
   * @return The file name of the TabletFile
   */
  String getFileName();

  /**
   * @return The path of the TabletFile
   */
  Path getPath();
}
