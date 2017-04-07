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
package org.apache.accumulo.server.fs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 */
public class ViewFSUtils {

  public static final String VIEWFS_CLASSNAME = "org.apache.hadoop.fs.viewfs.ViewFileSystem";

  public static boolean isViewFSSupported() {
    try {
      Class.forName(VIEWFS_CLASSNAME);
      return true;
    } catch (ClassNotFoundException e) {
      return false;
    }
  }

  public static boolean isViewFS(Path source, Configuration conf) throws IOException {
    return isViewFS(source.getFileSystem(conf));
  }

  public static boolean isViewFS(FileSystem fs) {
    return fs.getClass().getName().equals(VIEWFS_CLASSNAME);
  }

  public static Path matchingFileSystem(Path source, String[] options, Configuration conf) throws IOException {

    if (!isViewFS(source, conf))
      throw new IllegalArgumentException("source " + source + " is not view fs");

    String sourceUriPath = source.toUri().getPath();

    Path match = null;
    int matchPrefixLen = 0;

    // find the option with the longest commmon path prefix
    for (String option : options) {
      Path optionPath = new Path(option);
      if (isViewFS(optionPath, conf)) {
        String optionUriPath = optionPath.toUri().getPath();

        int commonPrefixLen = 0;
        for (int i = 0; i < Math.min(sourceUriPath.length(), optionUriPath.length()); i++) {
          if (sourceUriPath.charAt(i) == optionUriPath.charAt(i)) {
            if (sourceUriPath.charAt(i) == '/')
              commonPrefixLen++;
          } else {
            break;
          }
        }

        if (commonPrefixLen > matchPrefixLen) {
          matchPrefixLen = commonPrefixLen;
          match = optionPath;
        } else if (match != null && commonPrefixLen == matchPrefixLen && optionPath.depth() < match.depth()) {
          // take path with less depth when match perfix length is the same
          match = optionPath;
        }
      }
    }

    return match;
  }

}
