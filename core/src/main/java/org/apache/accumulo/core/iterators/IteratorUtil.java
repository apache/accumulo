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
package org.apache.accumulo.core.iterators;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;

/**
 * Utility class for Iterators.
 */
public class IteratorUtil {
  /**
   * Even though this type is not in a public API package, its used by methods in the public API.
   * Therefore it should be treated as public API and should not reference any non public API types.
   * Also this type can not be moved.
   */
  public enum IteratorScope {
    majc, minc, scan
  }

  /**
   * Maximize the Start Key timestamp.
   */
  public static Range maximizeStartKeyTimeStamp(Range range) {
    Range seekRange = range;

    if (range.getStartKey() != null) {
      Key seekKey = range.getStartKey();
      if (range.getStartKey().getTimestamp() != Long.MAX_VALUE) {
        seekKey = new Key(seekRange.getStartKey());
        seekKey.setTimestamp(Long.MAX_VALUE);
        seekRange = new Range(seekKey, true, range.getEndKey(), range.isEndKeyInclusive());
      } else if (!range.isStartKeyInclusive()) {
        seekRange = new Range(seekKey, true, range.getEndKey(), range.isEndKeyInclusive());
      }
    }

    return seekRange;
  }

  /**
   * Minimize the endKey Timestamp.
   */
  public static Range minimizeEndKeyTimeStamp(Range range) {
    Range seekRange = range;

    if (range.getEndKey() != null) {
      Key seekKey = seekRange.getEndKey();
      if (range.getEndKey().getTimestamp() != Long.MIN_VALUE) {
        seekKey = new Key(seekRange.getEndKey());
        seekKey.setTimestamp(Long.MIN_VALUE);
        seekRange = new Range(range.getStartKey(), range.isStartKeyInclusive(), seekKey, true);
      } else if (!range.isEndKeyInclusive()) {
        seekRange = new Range(range.getStartKey(), range.isStartKeyInclusive(), seekKey, true);
      }
    }

    return seekRange;
  }
}
