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
package org.apache.accumulo.manager.tableOps.bulkVer2;

import java.io.Serializable;
import java.util.ArrayList;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.TKeyExtent;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.hadoop.io.Text;

/**
 * Package private class to hold all the information used for bulk import2
 */
class BulkInfo implements Serializable {

  static enum NodeType {
    CONTIGUOUS_RANGE, SKIP_DISTANCE
  }

  static class MetadataRangeInfoNode implements Serializable {
    private static final long serialVersionUID = 1L;

    private final NodeType type;
    private final TKeyExtent rangeStart, rangeEnd;
    private final int tablets;

    public MetadataRangeInfoNode(KeyExtent start, KeyExtent end, int numTablets) {
      type = NodeType.CONTIGUOUS_RANGE;
      rangeStart = start.toThrift();
      rangeEnd = end.toThrift();
      tablets = numTablets;
    }

    public MetadataRangeInfoNode(int distance) {
      type = NodeType.SKIP_DISTANCE;
      rangeStart = null;
      rangeEnd = null;
      tablets = distance;
    }

    public NodeType getType() {
      return type;
    }

    public TKeyExtent getRangeStart() {
      return rangeStart;
    }

    public TKeyExtent getRangeEnd() {
      return rangeEnd;
    }

    public int getNumTablets() {
      return tablets;
    }

    @Override
    public String toString() {
      return type == NodeType.SKIP_DISTANCE ? "Skip Distance: " + tablets
          : "Tablet Range: " + KeyExtent.fromThrift(rangeStart) + " -> "
              + KeyExtent.fromThrift(rangeEnd) + " tablets: " + tablets;
    }

  }

  static class MetadataRanges extends ArrayList<MetadataRangeInfoNode> {
    private static final long serialVersionUID = 1L;

    public int findNodeForExtent(KeyExtent ke, int startPosition) {
      for (int i = startPosition; i < size(); i++) {
        MetadataRangeInfoNode node = get(i);
        if (node.getType() == NodeType.CONTIGUOUS_RANGE) {
          KeyExtent start = KeyExtent.fromThrift(node.getRangeStart());
          KeyExtent end = KeyExtent.fromThrift(node.getRangeEnd());
          KeyExtent metaTabletRange =
              new KeyExtent(start.tableId(), start.prevEndRow(), end.endRow());
          if (metaTabletRange.contains(ke)) {
            return i;
          }
        }
      }
      return -1;
    }

    public int getTabletDistance(int startNode, int endNode) {
      int tabletsToSkip = 0;
      for (int i = startNode; i < endNode; i++) {
        tabletsToSkip = get(i).getNumTablets();
      }
      return tabletsToSkip;
    }

    public Text getRangeStartPrevEndRow() {
      Text start = null;
      for (int i = 0; i < size(); i++) {
        MetadataRangeInfoNode node = get(i);
        if (node.getType() == NodeType.CONTIGUOUS_RANGE) {
          start = KeyExtent.fromThrift(node.getRangeStart()).prevEndRow();
          break;
        }
      }
      return start;
    }

    public Text getRangeEndRow() {
      Text end = null;
      for (int i = size() - 1; i >= 0; i--) {
        MetadataRangeInfoNode node = get(i);
        if (node.getType() == NodeType.CONTIGUOUS_RANGE) {
          end = KeyExtent.fromThrift(node.getRangeEnd()).endRow();
          break;
        }
      }
      return end;
    }
  }

  private static final long serialVersionUID = 1L;

  TableId tableId;
  String sourceDir;
  String bulkDir;
  boolean setTime;
  TableState tableState;
  // firstSplit and lastSplit describe the min and max splits in the table that overlap the bulk
  // imported data
  byte[] firstSplit;
  byte[] lastSplit;
  // List of metadata tablet ranges that files will be loaded into. This list contains nodes
  // that either represent a contiguous range of metadata tablets or the number of metadata
  // tablets to the next set of contiguous range of metadata tablets. For Accumulo versions
  // prior to 2.1 we need to be concerned about the size of the serialized repo. In 4.0,
  // with bulk import fate transactions going into the new fate table, and not ZooKeeper,
  // we likely don't have a limit on serialized Fate repo size
  MetadataRanges metadataRangesInfo;
}
