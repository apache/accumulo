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
package org.apache.accumulo.server.fs;

import java.util.Objects;

import com.google.common.base.Preconditions;

public enum FileTypePrefix {

  FLUSH(
      "F"),
  BULK_IMPORT("I"),
  COMPACTION("C"),
  FULL_COMPACTION("A"),
  MERGING_MINOR_COMPACTION("M");

  private final String filePrefix;

  private FileTypePrefix(String prefix) {
    this.filePrefix = prefix;
  }

  public String getPrefix() {
    return filePrefix;
  }

  public String createFileName(String fileName) {
    Objects.requireNonNull(fileName, "filename must be supplied");
    Preconditions.checkArgument(!fileName.isBlank(), "Empty filename supplied");
    if (this == MERGING_MINOR_COMPACTION) {
      throw new IllegalStateException(
          "Unable to create filename with MERGING_MINOR_COMPACTION prefix");
    }
    return filePrefix + fileName;
  }

  public static FileTypePrefix fromPrefix(String prefix) {
    Objects.requireNonNull(prefix, "prefix must be supplied");
    for (FileTypePrefix fp : values()) {
      if (fp.filePrefix.equals(prefix)) {
        return fp;
      }
    }
    throw new IllegalArgumentException("Unknown prefix type: " + prefix);

  }

  public static FileTypePrefix fromFileName(String fileName) {
    Objects.requireNonNull(fileName, "file name must be supplied");
    Preconditions.checkArgument(!fileName.isBlank(), "Empty filename supplied");
    String firstChar = fileName.substring(0, 1);
    if (!firstChar.equals(firstChar.toUpperCase())) {
      throw new IllegalArgumentException(
          "Expected first character of file name to be upper case, name: " + fileName);
    }
    return fromPrefix(firstChar);
  }

}
