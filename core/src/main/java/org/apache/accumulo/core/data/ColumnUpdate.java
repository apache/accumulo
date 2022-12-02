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
package org.apache.accumulo.core.data;

import java.util.Arrays;

/**
 * A single column and value pair within a {@link Mutation}.
 */
public class ColumnUpdate {

  private byte[] columnFamily;
  private byte[] columnQualifier;
  private byte[] columnVisibility;
  private long timestamp;
  private boolean hasTimestamp;
  private byte[] val;
  private boolean deleted;

  /**
   * Creates a new column update.
   *
   * @param cf column family
   * @param cq column qualifier
   * @param cv column visibility
   * @param hasts true if the update specifies a timestamp
   * @param ts timestamp
   * @param deleted delete marker
   * @param val cell value
   */
  public ColumnUpdate(byte[] cf, byte[] cq, byte[] cv, boolean hasts, long ts, boolean deleted,
      byte[] val) {
    this.columnFamily = cf;
    this.columnQualifier = cq;
    this.columnVisibility = cv;
    this.hasTimestamp = hasts;
    this.timestamp = ts;
    this.deleted = deleted;
    this.val = val;
  }

  /**
   * Gets whether this update specifies a timestamp.
   *
   * @return true if this update specifies a timestamp
   */
  public boolean hasTimestamp() {
    return hasTimestamp;
  }

  /**
   * Gets the column family for this update. Not a defensive copy.
   *
   * @return column family
   */
  public byte[] getColumnFamily() {
    return columnFamily;
  }

  /**
   * Gets the column qualifier for this update. Not a defensive copy.
   *
   * @return column qualifier
   */
  public byte[] getColumnQualifier() {
    return columnQualifier;
  }

  /**
   * Gets the column visibility for this update.
   *
   * @return column visibility
   */
  public byte[] getColumnVisibility() {
    return columnVisibility;
  }

  /**
   * Gets the timestamp for this update.
   *
   * @return timestamp
   */
  public long getTimestamp() {
    return this.timestamp;
  }

  /**
   * Gets the delete marker for this update.
   *
   * @return delete marker
   */
  public boolean isDeleted() {
    return this.deleted;
  }

  /**
   * Gets the cell value for this update.
   *
   * @return cell value
   */
  public byte[] getValue() {
    return this.val;
  }

  @Override
  public String toString() {
    return Arrays.toString(columnFamily) + ":" + Arrays.toString(columnQualifier) + " ["
        + Arrays.toString(columnVisibility) + "] " + (hasTimestamp ? timestamp : "NO_TIME_STAMP")
        + " " + Arrays.toString(val) + " " + deleted;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof ColumnUpdate)) {
      return false;
    }
    ColumnUpdate upd = (ColumnUpdate) obj;
    return Arrays.equals(getColumnFamily(), upd.getColumnFamily())
        && Arrays.equals(getColumnQualifier(), upd.getColumnQualifier())
        && Arrays.equals(getColumnVisibility(), upd.getColumnVisibility())
        && isDeleted() == upd.isDeleted() && Arrays.equals(getValue(), upd.getValue())
        && hasTimestamp() == upd.hasTimestamp() && getTimestamp() == upd.getTimestamp();
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(columnFamily) + Arrays.hashCode(columnQualifier)
        + Arrays.hashCode(columnVisibility)
        + (hasTimestamp ? (Boolean.TRUE.hashCode() + Long.valueOf(timestamp).hashCode())
            : Boolean.FALSE.hashCode())
        + (deleted ? Boolean.TRUE.hashCode() : (Boolean.FALSE.hashCode() + Arrays.hashCode(val)));
  }
}
