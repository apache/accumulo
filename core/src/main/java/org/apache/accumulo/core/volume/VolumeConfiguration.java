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
package org.apache.accumulo.core.volume;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class VolumeConfiguration {

  public static FileSystem fileSystemForPath(String path, Configuration conf) throws IOException {
    return path.contains(":") ? new Path(path).getFileSystem(conf) : FileSystem.get(conf);
  }

  public static Set<String> getVolumeUris(AccumuloConfiguration conf) {
    String volumes = conf.get(Property.INSTANCE_VOLUMES);
    if (volumes == null || volumes.isBlank()) {
      throw new IllegalArgumentException(
          "Missing required property " + Property.INSTANCE_VOLUMES.getKey());
    }
    String[] volArray = volumes.split(",");
    LinkedHashSet<String> deduplicated =
        Arrays.stream(volArray).map(VolumeConfiguration::normalizeVolume)
            .collect(Collectors.toCollection(LinkedHashSet::new));
    if (deduplicated.size() < volArray.length) {
      throw new IllegalArgumentException(
          Property.INSTANCE_VOLUMES.getKey() + " contains duplicate volumes (" + volumes + ")");
    }
    return deduplicated;
  }

  private static String normalizeVolume(String volume) {
    if (volume == null || volume.isBlank() || !volume.contains(":")) {
      throw new IllegalArgumentException("Expected fully qualified URI for "
          + Property.INSTANCE_VOLUMES.getKey() + " got " + volume);
    }
    try {
      // pass through URI to unescape hex encoded chars (e.g. convert %2C to "," char)
      return new Path(new URI(volume.strip())).toString();
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(Property.INSTANCE_VOLUMES.getKey() + " contains '" + volume
          + "' which has a syntax error", e);
    }
  }

}
