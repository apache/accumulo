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

  /**
   * The prefix used when an RFile is first written from memory as the result of a minor compaction
   * (a.k.a. 'flush')
   */
  FLUSH('F'),

  /**
   * The prefix for imported files as the result of a bulk import
   */
  BULK_IMPORT('I'),

  /**
   * The prefix used for files created as the result of a routine major compaction
   */
  COMPACTION('C'),

  /**
   * The prefix used for files created as the result of a major compaction that included all files
   * for a tablet
   */
  FULL_COMPACTION('A'),

  /**
   * The prefix used for files created as the result of a merging minor compaction (a removed
   * feature, but files may still be present with the name)
   */
  MERGING_MINOR_COMPACTION('M');

  private final char filePrefix;

  private FileTypePrefix(char prefix) {
    this.filePrefix = prefix;
  }

  public char getPrefix() {
    return filePrefix;
  }

  public String createFileName(String fileSuffix) {
    Objects.requireNonNull(fileSuffix, "fileSuffix must be supplied");
    Preconditions.checkArgument(!fileSuffix.isBlank(), "Empty fileSuffix supplied");
    if (this == MERGING_MINOR_COMPACTION) {
      throw new IllegalStateException("Unable to create filename for " + this.name());
    }
    return filePrefix + fileSuffix;
  }

  public static FileTypePrefix fromPrefix(char prefix) {
    for (FileTypePrefix fp : values()) {
      if (fp.filePrefix == prefix) {
        return fp;
      }
    }
    throw new IllegalArgumentException("Unknown prefix type: " + prefix);

  }

  public static FileTypePrefix fromFileName(String fileName) {
    Objects.requireNonNull(fileName, "file name must be supplied");
    Preconditions.checkArgument(!fileName.isBlank(), "Empty filename supplied");
    char firstChar = fileName.charAt(0);
    if (!Character.isUpperCase(firstChar)) {
      throw new IllegalArgumentException(
          "Expected first character of file name to be upper case, name: " + fileName);
    }
    return fromPrefix(firstChar);
  }

}
