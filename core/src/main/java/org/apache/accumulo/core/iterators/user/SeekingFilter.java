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
package org.apache.accumulo.core.iterators.user;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;

import java.io.IOException;
import java.util.Collection;

public abstract class SeekingFilter extends WrappingIterator {
  public enum IncludeResult {
    INCLUDE, SKIP
  }

  public enum AdvanceResult {
    NEXT, NEXT_CQ, NEXT_CF, NEXT_ROW, USE_HINT
  }

  public static class FilterResult {
    final IncludeResult include;
    final AdvanceResult advance;

    public FilterResult(IncludeResult include, AdvanceResult advance) {
      this.include = include;
      this.advance = advance;
    }
  }

  public abstract FilterResult filter(Key k, Value v);

  public abstract Key getNextKeyHint(Key k, Value v);

  private Collection<ByteSequence> columnFamilies;
  private boolean inclusive;
  private Range seekRange;

  private AdvanceResult advance;

  @Override
  public void next() throws IOException {
    findTop();
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    super.seek(range, columnFamilies, inclusive);
    advance = null;
    this.columnFamilies = columnFamilies;
    this.inclusive = inclusive;
    seekRange = range;
    findTop();
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    SeekingFilter newInstance;
    try {
      newInstance = this.getClass().newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    newInstance.setSource(getSource().deepCopy(env));
    return newInstance;
  }

  protected void findTop() throws IOException {
    SortedKeyValueIterator<Key,Value> src = getSource();
    // advance could be null if we've just been seeked
    if (src.hasTop() && advance != null) {
      advanceSource(src, advance);
    }
    advance = null;
    while (src.hasTop()) {
      if (src.getTopKey().isDeleted()) {
        // as per. o.a.a.core.iterators.Filter, deleted keys always pass through the filter.
        advance = AdvanceResult.NEXT;
        return;
      }
      FilterResult f = filter(src.getTopKey(), src.getTopValue());
      if (f.include == IncludeResult.INCLUDE) {
        // advance will be processed next time findTop is called
        advance = f.advance;
        break;
      } else {
        advanceSource(src, advance);
      }
    }
  }

  private void advanceSource(SortedKeyValueIterator<Key,Value> src, AdvanceResult adv) throws IOException {
    Key topKey = src.getTopKey();
    Range advRange = null;
    switch (adv) {
      case NEXT:
        src.next();
        return;
      case NEXT_CQ:
        advRange = new Range(topKey.followingKey(PartialKey.ROW_COLFAM_COLQUAL), null);
        break;
      case NEXT_CF:
        advRange = new Range(topKey.followingKey(PartialKey.ROW_COLFAM), null);
        break;
      case NEXT_ROW:
        advRange = new Range(topKey.followingKey(PartialKey.ROW), null);
        break;
      case USE_HINT:
        Value topVal = src.getTopValue();
        Key hintKey = getNextKeyHint(topKey, topVal);
        if (hintKey != null) {
          advRange = new Range(hintKey, null);
        } else {
          throw new IOException("Filter returned USE_HINT for " + topKey + " but no hint available.");
        }
        break;
    }
    if (advRange == null) {
      throw new IOException("Unable to determine range to advance to for AdvanceResult " + adv);
    }
    src.seek(advRange.clip(seekRange), columnFamilies, inclusive);
  }
}
