/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.server.gc;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily;
import org.apache.accumulo.core.volume.Volume;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.hadoop.fs.Path;

public class GcVolumeUtil {
  // AGCAV : Accumulo Garbage Collector All Volumes
  private static final String ALL_VOLUMES_PREFIX = "agcav:/";

  public static String getDeleteTabletOnAllVolumesUri(TableId tableId, String dirName) {
    ServerColumnFamily.validateDirCol(dirName);
    return ALL_VOLUMES_PREFIX + ServerConstants.TABLE_DIR + Path.SEPARATOR + tableId
        + Path.SEPARATOR + dirName;
  }

  public static Collection<Path> expandAllVolumesUri(VolumeManager fs, Path path) {
    if (path.toString().startsWith(ALL_VOLUMES_PREFIX)) {
      String relPath = path.toString().substring(ALL_VOLUMES_PREFIX.length());

      Collection<Volume> volumes = fs.getVolumes();

      ArrayList<Path> ret = new ArrayList<>(volumes.size());
      for (Volume vol : volumes) {
        Path volPath = vol.prefixChild(relPath);
        ret.add(volPath);
      }

      return ret;
    } else {
      return Collections.singleton(path);
    }
  }

  public static boolean isAllVolumesUri(Path path) {
    return path.toString().startsWith(ALL_VOLUMES_PREFIX);
  }
}
