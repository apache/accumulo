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
package org.apache.accumulo.core.zookeeper;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.file.FileUtil;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

public class ZooUtil extends org.apache.accumulo.fate.zookeeper.ZooUtil {
  
  private static final Logger log = Logger.getLogger(ZooUtil.class);
  
  public static String getRoot(final Instance instance) {
    return getRoot(instance.getInstanceID());
  }
  
  public static String getRoot(final String instanceId) {
    return Constants.ZROOT + "/" + instanceId;
  }
  
  /**
   * Utility to support certain client side utilities to minimize command-line options.
   */
  public static String getInstanceIDFromHdfs(Path instanceDirectory) {
    try {
      @SuppressWarnings("deprecation")
      FileSystem fs = FileUtil.getFileSystem(CachedConfiguration.getInstance(), AccumuloConfiguration.getSiteConfiguration());
      FileStatus[] files = null;
      try {
        files = fs.listStatus(instanceDirectory);
      } catch (FileNotFoundException ex) {
        // ignored
      }
      log.debug("Trying to read instance id from " + instanceDirectory);
      if (files == null || files.length == 0) {
        log.error("unable obtain instance id at " + instanceDirectory);
        throw new RuntimeException("Accumulo not initialized, there is no instance id at " + instanceDirectory);
      } else if (files.length != 1) {
        log.error("multiple potential instances in " + instanceDirectory);
        throw new RuntimeException("Accumulo found multiple possible instance ids in " + instanceDirectory);
      } else {
        String result = files[0].getPath().getName();
        return result;
      }
    } catch (IOException e) {
      throw new RuntimeException("Accumulo not initialized, there is no instance id at " + instanceDirectory, e);
    }
  }
}
