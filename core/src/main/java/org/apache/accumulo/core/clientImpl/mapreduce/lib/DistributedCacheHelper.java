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
package org.apache.accumulo.core.clientImpl.mapreduce.lib;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * Evolved to replace org.apache.hadoop.mapreduce.filecache.DistributedCache
 *
 * @since 1.6.0
 */
public class DistributedCacheHelper {
  // properties pulled from org.apache.hadoop.mapreduce.MRJobConfig
  public static final String CACHE_FILES = "mapreduce.job.cache.files";
  public static final String CACHE_LOCALFILES = "mapreduce.job.cache.local.files";

  public static void addCacheFile(URI uri, Configuration conf) {
    String files = conf.get(CACHE_FILES);
    conf.set(CACHE_FILES, files == null ? uri.toString() : files + "," + uri.toString());
  }

  public static URI[] getCacheFiles(Configuration conf) {
    return stringToURI(conf.getStrings(CACHE_FILES));
  }

  public static Path[] getLocalCacheFiles(Configuration conf) {
    return stringToPath(conf.getStrings(CACHE_LOCALFILES));
  }

  private static Path[] stringToPath(String[] str) {
    if (str == null)
      return null;
    return Arrays.stream(str).map(Path::new).toArray(Path[]::new);
  }

  private static URI[] stringToURI(String[] str) {
    if (str == null)
      return null;
    return Arrays.stream(str).map(s -> {
      try {
        return new URI(s);
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException("Failed to create uri for " + s, e);
      }
    }).toArray(URI[]::new);
  }
}
