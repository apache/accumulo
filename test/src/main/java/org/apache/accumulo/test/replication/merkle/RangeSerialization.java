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
package org.apache.accumulo.test.replication.merkle;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

/**
 *
 */
public class RangeSerialization {
  private static final Text EMPTY = new Text(new byte[0]);

  public static Range toRange(Key key) {
    Text holder = new Text();
    key.getRow(holder);
    Key startKey;
    if (0 == holder.getLength()) {
      startKey = null;
    } else {
      startKey = new Key(holder);
    }

    key.getColumnQualifier(holder);
    Key endKey;
    if (0 == holder.getLength()) {
      endKey = null;
    } else {
      endKey = new Key(holder);
    }

    // Don't be inclusive for no bounds on a Range
    return new Range(startKey, startKey != null, endKey, endKey != null);
  }

  public static Key toKey(Range range) {
    Text row = getRow(range);
    return new Key(row, EMPTY, getColumnQualifier(range));
  }

  public static Mutation toMutation(Range range, Value v) {
    Text row = getRow(range);
    Mutation m = new Mutation(row);
    m.put(EMPTY, getColumnQualifier(range), v);
    return m;
  }

  public static Text getRow(Range range) {
    return range.isInfiniteStartKey() ? EMPTY : range.getStartKey().getRow();
  }

  public static Text getColumnQualifier(Range range) {
    return range.isInfiniteStopKey() ? EMPTY : range.getEndKey().getRow();
  }
}
