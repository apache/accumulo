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

import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.TabletIdImpl;
import org.apache.hadoop.io.Text;

/**
 * A TabletId provides the information needed to uniquely identify a tablet.
 *
 * @since 1.7.0
 */
public interface TabletId extends Comparable<TabletId> {

  /**
   * Return a TabletId object for the provided TableId in range of (prevEndRow, endRow]. If the
   * parameters prevEndRow and/or endRow are null, they represent a tablet over the range of -inf
   * and/or inf respectively.
   *
   * @param tableId the ID for a table
   * @param endRow the last row in this tablet, or null if this is the last tablet in this table
   * @param prevEndRow the last row in the immediately preceding tablet for the table, or null if
   *        this represents the first tablet in this table
   * @return Return the new {@link TabletId} object
   * @since 2.1.4
   */
  static TabletId of(TableId tableId, Text endRow, Text prevEndRow) {
    KeyExtent ke = new KeyExtent(tableId, endRow, prevEndRow);
    return new TabletIdImpl(ke);
  }

  /**
   * @see #of(TableId, Text, Text)
   * @since 2.1.4
   */
  static TabletId of(TableId tableId, String endRow, String prevEndRow) {
    KeyExtent ke;
    if (endRow == null && prevEndRow == null) {
      ke = new KeyExtent(tableId, null, null);
    } else if (endRow == null) {
      ke = new KeyExtent(tableId, null, new Text(prevEndRow));
    } else if (prevEndRow == null) {
      ke = new KeyExtent(tableId, new Text(endRow), null);
    } else {
      ke = new KeyExtent(tableId, new Text(endRow), new Text(prevEndRow));
    }

    return new TabletIdImpl(ke);
  }

  /**
   * @see #of(TableId, Text, Text)
   * @since 2.1.4
   */
  static TabletId of(TableId tableId, byte[] endRow, byte[] prevEndRow) {
    KeyExtent ke;
    if (endRow == null && prevEndRow == null) {
      ke = new KeyExtent(tableId, null, null);
    } else if (endRow == null) {
      ke = new KeyExtent(tableId, null, new Text(prevEndRow));
    } else if (prevEndRow == null) {
      ke = new KeyExtent(tableId, new Text(endRow), null);
    } else {
      ke = new KeyExtent(tableId, new Text(endRow), new Text(prevEndRow));
    }

    return new TabletIdImpl(ke);
  }

  /**
   * @since 2.1.0
   */
  TableId getTable();

  /**
   * @deprecated use {@link #getTable()} and {@link TableId#canonical()} instead
   */
  @Deprecated(since = "2.1.0")
  Text getTableId();

  Text getEndRow();

  Text getPrevEndRow();

  /**
   * @return a range based on the row range of the tablet. The range will cover
   *         {@code (<prev end row>, <end row>]}.
   * @since 1.8.0
   */
  Range toRange();
}
