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
package org.apache.accumulo.core.file;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import com.google.common.base.Preconditions;

public enum FilePrefix {

  ALL("*"),
  MINOR_COMPACTION("F"),
  BULK_IMPORT("I"),
  MAJOR_COMPACTION("C"),
  MAJOR_COMPACTION_ALL_FILES("A"),
  MERGING_MINOR_COMPACTION("M");

  final String prefix;

  FilePrefix(String prefix) {
    this.prefix = prefix;
  }

  public String toPrefix() {
    return this.prefix;
  }

  public String createFileName(String fileName) {
    Objects.requireNonNull(fileName, "filename must be supplied");
    Preconditions.checkArgument(!fileName.isBlank(), "Empty filename supplied");
    if (this == ALL || this == MERGING_MINOR_COMPACTION) {
      throw new IllegalStateException(
          "Unable to create filename with ALL, MERGING_MINOR_COMPACTION, or UNKNOWN prefix");
    }
    return prefix + fileName;
  }

  public static FilePrefix fromPrefix(String prefix) {
    Objects.requireNonNull(prefix, "prefix must be supplied");
    Preconditions.checkArgument(!prefix.isBlank(), "Empty prefix supplied");
    Preconditions.checkArgument(prefix.length() == 1, "Invalid prefix supplied: " + prefix);
    switch (prefix.toUpperCase()) {
      case "A":
        return MAJOR_COMPACTION_ALL_FILES;
      case "C":
        return MAJOR_COMPACTION;
      case "F":
        return MINOR_COMPACTION;
      case "I":
        return BULK_IMPORT;
      case "M":
        return MERGING_MINOR_COMPACTION;
      default:
        throw new IllegalArgumentException("Unknown prefix type: " + prefix);
    }
  }

  public static FilePrefix fromFileName(String fileName) {
    Objects.requireNonNull(fileName, "file name must be supplied");
    Preconditions.checkArgument(!fileName.isBlank(), "Empty filename supplied");
    String firstChar = fileName.substring(0, 1);
    if (!firstChar.equals(firstChar.toUpperCase())) {
      throw new IllegalArgumentException(
          "Expected first character of file name to be upper case, name: " + fileName);
    }
    return fromPrefix(firstChar);
  }

  public static EnumSet<FilePrefix> typesFromList(String list) {
    final EnumSet<FilePrefix> dropCacheFilePrefixes;
    if (!list.isBlank()) {
      if (list.contains("*")) {
        dropCacheFilePrefixes = EnumSet.of(FilePrefix.ALL);
      } else {
        Set<FilePrefix> set = new HashSet<>();
        String[] prefixes = list.trim().split(",");
        for (String p : prefixes) {
          set.add(FilePrefix.fromPrefix(p.trim().toUpperCase()));
        }
        dropCacheFilePrefixes = EnumSet.copyOf(set);
      }
    } else {
      dropCacheFilePrefixes = EnumSet.noneOf(FilePrefix.class);
    }
    return dropCacheFilePrefixes;
  }

}
