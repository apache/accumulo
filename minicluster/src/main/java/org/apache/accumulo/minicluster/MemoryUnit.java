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
package org.apache.accumulo.minicluster;

/**
 * @since 1.6.0
 */
public enum MemoryUnit {

  BYTE(1L, ""),
  KILOBYTE(1024L, "K"),
  MEGABYTE(1024 * 1024L, "M"),
  GIGABYTE(1024 * 1024 * 1024L, "G");

  private final long multiplier;
  private final String suffix;

  private MemoryUnit(long multiplier, String suffix) {
    this.multiplier = multiplier;
    this.suffix = suffix;
  }

  public long toBytes(long memory) {
    return memory * multiplier;
  }

  public String suffix() {
    return suffix;
  }

  public static MemoryUnit fromSuffix(String suffix) {
    for (MemoryUnit memoryUnit : MemoryUnit.values()) {
      if (memoryUnit.suffix.equals(suffix)) {
        return memoryUnit;
      }
    }
    return null;
  }
}
