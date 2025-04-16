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

  ALL("*", false),
  MINOR_COMPACTION("F", true),
  BULK_IMPORT("I", true),
  MAJOR_COMPACTION("C", true),
  MAJOR_COMPACTION_ALL_FILES("A", true),
  MERGING_MINOR_COMPACTION("M", false);

  final String prefix;
  final boolean canCreateFiles;

  FilePrefix(String prefix, boolean canCreateFiles) {
    this.prefix = prefix;
    this.canCreateFiles = canCreateFiles;
  }

  public String toPrefix() {
    return this.prefix;
  }

  public String createFileName(String fileName) {
    Objects.requireNonNull(fileName, "filename must be supplied");
    Preconditions.checkArgument(!fileName.isBlank(), "Empty filename supplied");
    if (!canCreateFiles) {
      throw new IllegalStateException("Unable to create filename with prefix: " + prefix);
    }
    return prefix + fileName;
  }

  public static FilePrefix fromPrefix(String prefix) {
    Objects.requireNonNull(prefix, "prefix must be supplied");
    Preconditions.checkArgument(!prefix.isBlank(), "Empty prefix supplied");
    Preconditions.checkArgument(prefix.length() == 1, "Invalid prefix supplied: " + prefix);
    for (FilePrefix fp : values()) {
      if (fp == ALL) {
        continue;
      }
      if (fp.prefix.equals(prefix)) {
        return fp;
      }
    }
    throw new IllegalArgumentException("Unknown prefix type: " + prefix);
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
    final EnumSet<FilePrefix> result;
    if (!list.isBlank()) {
      if (list.contains("*")) {
        result = EnumSet.of(FilePrefix.ALL);
      } else {
        Set<FilePrefix> set = new HashSet<>();
        String[] prefixes = list.trim().split(",");
        for (String p : prefixes) {
          set.add(FilePrefix.fromPrefix(p.trim().toUpperCase()));
        }
        result = EnumSet.copyOf(set);
      }
    } else {
      result = EnumSet.noneOf(FilePrefix.class);
    }
    return result;
  }

}
