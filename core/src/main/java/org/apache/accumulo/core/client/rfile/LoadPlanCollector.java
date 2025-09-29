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
package org.apache.accumulo.core.client.rfile;

import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.LoadPlan;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.hadoop.io.Text;

import com.google.common.base.Preconditions;

class LoadPlanCollector {

  private final LoadPlan.SplitResolver splitResolver;
  private boolean finished = false;
  private Text lgFirstRow;
  private Text lgLastRow;
  private Text firstRow;
  private Text lastRow;
  private Set<KeyExtent> overlappingExtents;
  private KeyExtent currentExtent;
  private long appended = 0;

  LoadPlanCollector(LoadPlan.SplitResolver splitResolver) {
    this.splitResolver = splitResolver;
    this.overlappingExtents = new HashSet<>();
  }

  LoadPlanCollector() {
    splitResolver = null;
    this.overlappingExtents = null;

  }

  private void appendNoSplits(Key key) {
    if (lgFirstRow == null) {
      lgFirstRow = key.getRow();
      lgLastRow = lgFirstRow;
    } else {
      var row = key.getRow();
      lgLastRow = row;
    }
  }

  private static final TableId FAKE_ID = TableId.of("123");

  private void appendSplits(Key key) {
    var row = key.getRow();
    if (currentExtent == null || !currentExtent.contains(row)) {
      var tableSplits = splitResolver.apply(row);
      var extent = new KeyExtent(FAKE_ID, tableSplits.getEndRow(), tableSplits.getPrevRow());
      Preconditions.checkState(extent.contains(row), "%s does not contain %s", tableSplits, row);
      if (currentExtent != null) {
        overlappingExtents.add(currentExtent);
      }
      currentExtent = extent;
    }
  }

  public void append(Key key) {
    if (splitResolver == null) {
      appendNoSplits(key);
    } else {
      appendSplits(key);
    }
    appended++;
  }

  public void startLocalityGroup() {
    if (lgFirstRow != null) {
      if (firstRow == null) {
        firstRow = lgFirstRow;
        lastRow = lgLastRow;
      } else {
        // take the minimum
        firstRow = firstRow.compareTo(lgFirstRow) < 0 ? firstRow : lgFirstRow;
        // take the maximum
        lastRow = lastRow.compareTo(lgLastRow) > 0 ? lastRow : lgLastRow;
      }
      lgFirstRow = null;
      lgLastRow = null;
    }
  }

  public LoadPlan getLoadPlan(String filename) {
    Preconditions.checkState(finished, "Attempted to get load plan before closing");

    if (appended == 0) {
      return LoadPlan.builder().build();
    }

    if (splitResolver == null) {
      return LoadPlan.builder().loadFileTo(filename, LoadPlan.RangeType.FILE, firstRow, lastRow)
          .build();
    } else {
      var builder = LoadPlan.builder();
      overlappingExtents.add(currentExtent);
      for (var extent : overlappingExtents) {
        builder.loadFileTo(filename, LoadPlan.RangeType.TABLE, extent.prevEndRow(),
            extent.endRow());
      }
      return builder.build();
    }
  }

  public void close() {
    finished = true;
    // compute the overall min and max rows
    startLocalityGroup();
  }
}
