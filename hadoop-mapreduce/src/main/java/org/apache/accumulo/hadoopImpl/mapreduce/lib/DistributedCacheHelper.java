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
package org.apache.accumulo.hadoopImpl.mapreduce.lib;

import static java.util.Objects.requireNonNull;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @since 1.6.0
 */
public class DistributedCacheHelper {

  /**
   * @since 1.6.0
   */
  @SuppressWarnings("deprecation")
  public static void addCacheFile(String path, String fragment, Configuration conf) {
    org.apache.hadoop.filecache.DistributedCache.addCacheFile(getUri(path, fragment), conf);
  }

  public static void addCacheFile(Job job, String path, String fragment) {
    job.addCacheFile(getUri(path, fragment));
  }

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN",
      justification = "arbitrary path provided by user, through MapReduce APIs")
  public static InputStream openCachedFile(String path, String fragment, Configuration conf) {
    // try to get it from the local copy provided by the distributed cache
    File tempFile = new File(fragment);
    if (tempFile.exists()) {
      try {
        return new FileInputStream(tempFile);
      } catch (FileNotFoundException e) {
        throw new AssertionError("FileNotFoundException after verifying file exists", e);
      }
    } else {

      // try to get token directly from HDFS path, without using the distributed cache
      try {
        return FileSystem.get(conf).open(new Path(path));
      } catch (IOException e) {
        throw new IllegalArgumentException("Unable to read file at DFS Path " + path, e);
      }
    }
  }

  private static URI getUri(String path, String fragment) {
    String uriString = requireNonNull(path) + "#" + requireNonNull(fragment);
    if (path.contains("#")) {
      throw new IllegalArgumentException("Path to cache cannot contain a URI fragment");
    }

    try {
      return new URI(uriString);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid URI for item to cache", e);
    }
  }

}
