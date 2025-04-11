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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public enum AccumuloFileType {

  ALL("*"),
  FLUSH("F"),
  BULK_IMPORT("I"),
  COMPACTION("C"),
  FULL_COMPACTION("A"),
  MERGING_MINOR_COMPACTION("M"),
  UNKNOWN("?");

  private static final Logger LOG = LoggerFactory.getLogger(AccumuloFileType.class);

  private final String filePrefix;

  private AccumuloFileType(String prefix) {
    this.filePrefix = prefix;
  }

  public String getPrefix() {
    return filePrefix;
  }

  public String createFileName(String fileName) {
    if (this == ALL || this == UNKNOWN) {
      throw new IllegalStateException("Unable to create filename with ALL or UNKNOWN prefix");
    }
    return filePrefix + fileName;
  }

  public static AccumuloFileType fromPrefix(String prefix) {
    Objects.requireNonNull(prefix, "prefix must be supplied");
    Preconditions.checkArgument(!prefix.isBlank(), "Empty prefix supplied");
    Preconditions.checkArgument(prefix.length() == 1, "Invalid prefix supplied: " + prefix);
    switch (prefix) {
      case "A":
        return FULL_COMPACTION;
      case "C":
        return COMPACTION;
      case "F":
        return FLUSH;
      case "I":
        return BULK_IMPORT;
      case "M":
        return MERGING_MINOR_COMPACTION;
      default:
        LOG.warn("Encountered unknown file prefix for file: {}", prefix);
        return UNKNOWN;
    }
  }

  public static AccumuloFileType fromFileName(String fileName) {
    Objects.requireNonNull(fileName, "file name must be supplied");
    Preconditions.checkArgument(!fileName.isBlank(), "Empty filename supplied");
    String firstChar = fileName.substring(0, 1);
    return fromPrefix(firstChar);
  }

}
