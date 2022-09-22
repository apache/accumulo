/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.server.gc;

import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;

import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.hadoop.fs.Path;

public class GcVolumeUtil {
  // AGCAV : Accumulo Garbage Collector All Volumes
  static final String ALL_VOLUMES_PREFIX = "agcav:/";

  public static Collection<Path> expandAllVolumesUri(VolumeManager fs, Path path) {
    if (path.toString().startsWith(ALL_VOLUMES_PREFIX)) {
      String relPath = path.toString().substring(ALL_VOLUMES_PREFIX.length());
      return fs.getVolumes().stream().map(vol -> vol.prefixChild(relPath))
          .collect(Collectors.toList());
    } else {
      return Collections.singleton(path);
    }
  }

  public static boolean isAllVolumesUri(Path path) {
    return path.toString().startsWith(ALL_VOLUMES_PREFIX);
  }
}
