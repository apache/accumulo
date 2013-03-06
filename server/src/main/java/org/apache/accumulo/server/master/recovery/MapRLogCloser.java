package org.apache.accumulo.server.master.recovery;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.log4j.Logger;

public class MapRLogCloser implements LogCloser {

  private static Logger log = Logger.getLogger(MapRLogCloser.class);

  @Override
  public long close(FileSystem fs, Path path) throws IOException {
    log.info("Recovering file " + path.toString() + " by changing permission to readonly");
    FsPermission roPerm = new FsPermission((short) 0444);
    try {
      fs.setPermission(path, roPerm);
      return 0;
    } catch (IOException ex) {
      log.error("error recovering lease ", ex);
      // lets do this again
      return 1000;
    }
  }

}
