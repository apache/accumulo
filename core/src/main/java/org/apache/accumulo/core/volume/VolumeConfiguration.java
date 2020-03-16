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
package org.apache.accumulo.core.volume;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class VolumeConfiguration {

  public static Volume getVolume(String path, Configuration conf, AccumuloConfiguration acuconf)
      throws IOException {
    if (requireNonNull(path).contains(":")) {
      // An absolute path
      return new VolumeImpl(new Path(path), conf);
    } else {
      // A relative path
      return getDefaultVolume(conf, acuconf);
    }
  }

  public static Volume getDefaultVolume(Configuration conf, AccumuloConfiguration acuconf)
      throws IOException {
    @SuppressWarnings("deprecation")
    String uri = acuconf.get(Property.INSTANCE_DFS_URI);

    // By default pull from INSTANCE_DFS_URI, falling back to the Hadoop defined
    // default filesystem (fs.defaultFS or the deprecated fs.default.name)
    if ("".equals(uri))
      return create(FileSystem.get(conf), acuconf);
    else
      try {
        return create(FileSystem.get(new URI(uri), conf), acuconf);
      } catch (URISyntaxException e) {
        throw new IOException(e);
      }
  }

  /**
   * @see org.apache.accumulo.core.volume.VolumeConfiguration#getVolumeUris(AccumuloConfiguration,Configuration)
   */
  @Deprecated
  public static String getConfiguredBaseDir(AccumuloConfiguration conf,
      Configuration hadoopConfig) {
    String singleNamespace = conf.get(Property.INSTANCE_DFS_DIR);
    String dfsUri = conf.get(Property.INSTANCE_DFS_URI);
    String baseDir;

    if (dfsUri == null || dfsUri.isEmpty()) {
      try {
        baseDir = FileSystem.get(hadoopConfig).getUri() + singleNamespace;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else {
      if (!dfsUri.contains(":"))
        throw new IllegalArgumentException("Expected fully qualified URI for "
            + Property.INSTANCE_DFS_URI.getKey() + " got " + dfsUri);
      baseDir = dfsUri + singleNamespace;
    }
    return baseDir;
  }

  public static Set<String> getVolumeUris(AccumuloConfiguration conf, Configuration hadoopConfig) {
    String ns = conf.get(Property.INSTANCE_VOLUMES);

    // preserve configuration order using LinkedHashSet
    ArrayList<String> configuredBaseDirs = new ArrayList<>();

    if (ns == null || ns.isEmpty()) {
      // Fall back to using the old config values
      configuredBaseDirs.add(getConfiguredBaseDir(conf, hadoopConfig));
    } else {
      String[] namespaces = ns.split(",");
      for (String namespace : namespaces) {
        if (!namespace.contains(":")) {
          throw new IllegalArgumentException("Expected fully qualified URI for "
              + Property.INSTANCE_VOLUMES.getKey() + " got " + namespace);
        }

        try {
          // pass through URI to unescape hex encoded chars (e.g. convert %2C to "," char)
          configuredBaseDirs.add(new Path(new URI(namespace)).toString());
        } catch (URISyntaxException e) {
          throw new IllegalArgumentException(Property.INSTANCE_VOLUMES.getKey() + " contains "
              + namespace + " which has a syntax error", e);
        }
      }
    }

    LinkedHashSet<String> deduplicated = new LinkedHashSet<>();
    deduplicated.addAll(configuredBaseDirs);
    if (deduplicated.isEmpty()) {
      throw new IllegalArgumentException(
          Property.INSTANCE_VOLUMES.getKey() + " contains no volumes (" + ns + ")");
    }
    if (deduplicated.size() < configuredBaseDirs.size()) {
      throw new IllegalArgumentException(
          Property.INSTANCE_VOLUMES.getKey() + " contains duplicate volumes (" + ns + ")");
    }
    return deduplicated;
  }

  public static Set<String> prefix(Set<String> bases, String suffix) {
    String actualSuffix = suffix.startsWith("/") ? suffix.substring(1) : suffix;
    return bases.stream().map(base -> base + (base.endsWith("/") ? "" : "/") + actualSuffix)
        .collect(Collectors.toCollection(LinkedHashSet::new));
  }

  /**
   * Create a Volume with the given FileSystem that writes to the default path
   *
   * @param fs
   *          A FileSystem to write to
   * @return A Volume instance writing to the given FileSystem in the default path
   */
  @SuppressWarnings("deprecation")
  public static <T extends FileSystem> Volume create(T fs, AccumuloConfiguration acuconf) {
    String dfsDir = acuconf.get(Property.INSTANCE_DFS_DIR);
    return new VolumeImpl(fs,
        dfsDir == null ? Property.INSTANCE_DFS_DIR.getDefaultValue() : dfsDir);
  }

}
