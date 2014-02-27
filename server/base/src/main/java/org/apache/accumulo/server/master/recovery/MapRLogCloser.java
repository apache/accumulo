/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.server.master.recovery;

import java.io.IOException;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.log4j.Logger;

public class MapRLogCloser implements LogCloser {

  private static Logger log = Logger.getLogger(MapRLogCloser.class);

  @Override
  public long close(AccumuloConfiguration conf, VolumeManager fs, Path path) throws IOException {
    log.info("Recovering file " + path.toString() + " by changing permission to readonly");
    FileSystem ns = fs.getVolumeByPath(path).getFileSystem();
    FsPermission roPerm = new FsPermission((short) 0444);
    try {
      ns.setPermission(path, roPerm);
      return 0;
    } catch (IOException ex) {
      log.error("error recovering lease ", ex);
      // lets do this again
      return 1000;
    }
  }

}
