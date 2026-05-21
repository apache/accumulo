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
package org.apache.accumulo.core.fate.zookeeper;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.hadoop.io.Text;

public class LockRange {
  private final KeyExtent range;

  private static final LockRange INF = new LockRange(null, null);

  private LockRange(Text startRow, Text endRow) {
    this.range = new KeyExtent(TableId.of("0"), endRow, startRow);
  }

  public Text getStartRow() {
    return range.prevEndRow();
  }

  public Text getEndRow() {
    return range.endRow();
  }

  public boolean overlaps(LockRange other) {
    return range.overlaps(other.range);
  }

  public boolean isInfinite() {
    return range.prevEndRow() == null && range.endRow() == null;
  }

  public boolean contains(LockRange widenedRange) {
    return range.contains(widenedRange.range);
  }

  public static LockRange of(String startRow, String endRow) {
    return of(startRow == null ? null : new Text(startRow),
        endRow == null ? null : new Text(endRow));
  }

  public static LockRange of(byte[] startRow, byte[] endRow) {
    return of(startRow == null ? null : new Text(startRow),
        endRow == null ? null : new Text(endRow));
  }

  public static LockRange of(KeyExtent extent) {
    return of(extent.prevEndRow(), extent.endRow());
  }

  public static LockRange of(Text startRow, Text endRow) {
    if (startRow == null && endRow == null) {
      return infinite();
    }

    return new LockRange(startRow, endRow);
  }

  public static LockRange infinite() {
    return INF;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof LockRange other) {
      return range.equals(other.range);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return range.hashCode();
  }

  @Override
  public String toString() {
    return "(" + range.prevEndRow() + "," + range.endRow() + "]";
  }
}
