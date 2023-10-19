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
package org.apache.accumulo.manager.tableOps.split;

import java.io.Serializable;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.hadoop.io.Text;

import com.google.common.base.Preconditions;

public class SplitInfo implements Serializable {
  private static final long serialVersionUID = 1L;

  private final TableId tableId;
  private final byte[] prevEndRow;
  private final byte[] endRow;
  private final byte[][] splits;

  SplitInfo(KeyExtent extent, SortedSet<Text> splits) {
    this.tableId = extent.tableId();
    this.prevEndRow = extent.prevEndRow() == null ? null : TextUtil.getBytes(extent.prevEndRow());
    this.endRow = extent.endRow() == null ? null : TextUtil.getBytes(extent.endRow());
    this.splits = new byte[splits.size()][];

    int index = 0;
    for (var split : splits) {
      Preconditions.checkArgument(extent.contains(split));
      this.splits[index] = TextUtil.getBytes(split);
      index++;
    }
  }

  private static Text toText(byte[] bytes) {
    return bytes == null ? null : new Text(bytes);
  }

  KeyExtent getOriginal() {
    return new KeyExtent(tableId, toText(endRow), toText(prevEndRow));
  }

  SortedSet<Text> getSplits() {
    SortedSet<Text> splits = new TreeSet<>();
    for (int i = 0; i < this.splits.length; i++) {
      splits.add(new Text(this.splits[i]));
    }
    return splits;
  }

  SortedSet<KeyExtent> getTablets() {

    Text prev = getOriginal().prevEndRow();

    TreeSet<KeyExtent> tablets = new TreeSet<>();

    for (var split : getSplits()) {
      var extent = new KeyExtent(getOriginal().tableId(), split, prev);
      prev = split;
      tablets.add(extent);
    }

    var extent = new KeyExtent(getOriginal().tableId(), getOriginal().endRow(), prev);
    tablets.add(extent);

    return tablets;
  }

}
