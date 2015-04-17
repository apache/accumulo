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
package org.apache.accumulo.core.data;

/**
 * Specifications for part of a {@link Key}.
 */
public enum PartialKey {
  ROW(1), ROW_COLFAM(2), ROW_COLFAM_COLQUAL(3), ROW_COLFAM_COLQUAL_COLVIS(4), ROW_COLFAM_COLQUAL_COLVIS_TIME(5),
  // everything with delete flag
  ROW_COLFAM_COLQUAL_COLVIS_TIME_DEL(6);

  int depth;

  private PartialKey(int depth) {
    this.depth = depth;
  }

  /**
   * Get a partial key specification by depth of the specification.
   *
   * @param depth
   *          depth of scope (i.e., number of fields included)
   * @return partial key
   * @throws IllegalArgumentException
   *           if no partial key has the given depth
   * @deprecated since 1.7.0
   */
  @Deprecated
  public static PartialKey getByDepth(int depth) {
    for (PartialKey d : PartialKey.values())
      if (depth == d.depth)
        return d;
    throw new IllegalArgumentException("Invalid legacy depth " + depth);
  }

  /**
   * Gets the depth of this partial key.
   *
   * @return depth
   */
  public int getDepth() {
    return depth;
  }
}
