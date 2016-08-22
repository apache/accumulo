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
package org.apache.accumulo.core.volume;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class VolumeConfiguration {

  public static Volume getVolume(String path, Configuration conf, AccumuloConfiguration acuconf) throws IOException {
    requireNonNull(path);

    if (path.contains(":")) {
      // An absolute path
      return create(new Path(path), conf);
    } else {
      // A relative path
      return getDefaultVolume(conf, acuconf);
    }
  }

  public static Volume getDefaultVolume(Configuration conf, AccumuloConfiguration acuconf) throws IOException {
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
  public static String getConfiguredBaseDir(AccumuloConfiguration conf, Configuration hadoopConfig) {
    String singleNamespace = conf.get(Property.INSTANCE_DFS_DIR);
    String dfsUri = conf.get(Property.INSTANCE_DFS_URI);
    String baseDir;

    if (dfsUri == null || dfsUri.isEmpty()) {
      try {
        baseDir = FileSystem.get(hadoopConfig).getUri().toString() + singleNamespace;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else {
      if (!dfsUri.contains(":"))
        throw new IllegalArgumentException("Expected fully qualified URI for " + Property.INSTANCE_DFS_URI.getKey() + " got " + dfsUri);
      baseDir = dfsUri + singleNamespace;
    }
    return baseDir;
  }

  /**
   * Compute the URIs to be used by Accumulo
   *
   */
  public static String[] getVolumeUris(AccumuloConfiguration conf) {
    return getVolumeUris(conf, CachedConfiguration.getInstance());
  }

  public static String[] getVolumeUris(AccumuloConfiguration conf, Configuration hadoopConfig) {
    String ns = conf.get(Property.INSTANCE_VOLUMES);

    String configuredBaseDirs[];

    if (ns == null || ns.isEmpty()) {
      // Fall back to using the old config values
      configuredBaseDirs = new String[] {getConfiguredBaseDir(conf, hadoopConfig)};
    } else {
      String namespaces[] = ns.split(",");
      configuredBaseDirs = new String[namespaces.length];
      int i = 0;
      for (String namespace : namespaces) {
        if (!namespace.contains(":")) {
          throw new IllegalArgumentException("Expected fully qualified URI for " + Property.INSTANCE_VOLUMES.getKey() + " got " + namespace);
        }

        try {
          // pass through URI to unescape hex encoded chars (e.g. convert %2C to "," char)
          configuredBaseDirs[i++] = new Path(new URI(namespace)).toString();
        } catch (URISyntaxException e) {
          throw new IllegalArgumentException(Property.INSTANCE_VOLUMES.getKey() + " contains " + namespace + " which has a syntax error", e);
        }
      }
    }

    return configuredBaseDirs;
  }

  public static String[] prefix(String bases[], String suffix) {
    if (suffix.startsWith("/"))
      suffix = suffix.substring(1);
    String result[] = new String[bases.length];
    for (int i = 0; i < bases.length; i++) {
      if (bases[i].endsWith("/")) {
        result[i] = bases[i] + suffix;
      } else {
        result[i] = bases[i] + "/" + suffix;
      }
    }
    return result;
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
    return new VolumeImpl(fs, null == dfsDir ? Property.INSTANCE_DFS_DIR.getDefaultValue() : dfsDir);
  }

  public static <T extends FileSystem> Volume create(T fs, String basePath) {
    return new VolumeImpl(fs, basePath);
  }

  public static Volume create(Path path, Configuration conf) throws IOException {
    return new VolumeImpl(path, conf);
  }

}
