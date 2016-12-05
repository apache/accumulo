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

package org.apache.accumulo.core.file.summary;

import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;

class SummaryInfo {

  Map<String,Long> summary;
  ByteSequence lastRow;
  int estimatedSize = 0;

  SummaryInfo(ByteSequence row, Map<String,Long> summary) {
    this.lastRow = row;
    this.summary = summary;
  }

  SummaryInfo(byte[] row, Map<String,Long> summary) {
    this.lastRow = new ArrayByteSequence(row);
    this.summary = summary;
  }

  int getEstimatedSize() {
    if (estimatedSize == 0) {
      int size = 0;
      Set<Entry<String,Long>> es = summary.entrySet();
      for (Entry<String,Long> entry : es) {
        size += entry.getKey().length();
        size += 8;
      }
      estimatedSize = size;
    }

    return estimatedSize;
  }

  ByteSequence getLastRow() {
    return lastRow;
  }
}
