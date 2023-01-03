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

public enum Prefix {

  BULK_IMPORT("I"), MINOR_COMPACTION("F"), MAJOR_COMPACTION("C"), MAJOR_COMPACTION_ALL_FILES("A");

  String prefix;

  Prefix(String prefix) {
    this.prefix = prefix;
  }

  public static Prefix fromPrefix(String prefix) {
    switch (prefix) {
      case "I":
        return BULK_IMPORT;
      case "F":
        return MINOR_COMPACTION;
      case "C":
        return MAJOR_COMPACTION;
      case "A":
        return MAJOR_COMPACTION_ALL_FILES;
      default:
        throw new IllegalArgumentException("Unknown prefix type: " + prefix);
    }
  }

  public String toPrefix() {
    return this.prefix;
  }

}
