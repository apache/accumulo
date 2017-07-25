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

import java.io.IOException;
import java.util.Collection;
import java.util.EnumMap;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for filters that can skip over key-value pairs which do not match their filter predicate. In addition to returning true/false to accept or reject
 * a kv pair, subclasses can return an extra field which indicates how far the source iterator should be advanced.
 *
 * Note that the behaviour of the negate option is different from the Filter class. If a KV pair fails the subclass' filter predicate and negate is true, then
 * the KV pair will pass the filter. However if the subclass advances the source past a bunch of KV pairs, all those pairs will be implicitly rejected and
 * negate will have no effect.
 *
 * @see org.apache.accumulo.core.iterators.Filter
 */
public abstract class SeekingFilter extends WrappingIterator {
  private static final Logger log = LoggerFactory.getLogger(SeekingFilter.class);

  protected static final String NEGATE = "negate";

  public enum AdvanceResult {
    NEXT, NEXT_CQ, NEXT_CF, NEXT_ROW, USE_HINT
  }

  public static class FilterResult {
    private static final EnumMap<AdvanceResult,FilterResult> PASSES = new EnumMap<>(AdvanceResult.class);
    private static final EnumMap<AdvanceResult,FilterResult> FAILS = new EnumMap<>(AdvanceResult.class);
    static {
      for (AdvanceResult ar : AdvanceResult.values()) {
        PASSES.put(ar, new FilterResult(true, ar));
        FAILS.put(ar, new FilterResult(false, ar));
      }
    }

    final boolean accept;
    final AdvanceResult advance;

    public FilterResult(boolean accept, AdvanceResult advance) {
      this.accept = accept;
      this.advance = advance;
    }

    public static FilterResult of(boolean accept, AdvanceResult advance) {
      return accept ? PASSES.get(advance) : FAILS.get(advance);
    }

    public String toString() {
      return "Acc: " + accept + " Adv: " + advance;
    }
  }

  /**
   * Subclasses must provide an implementation which examines the given key and value and determines (1) whether to accept the KV pair and (2) how far to
   * advance the source iterator past the key.
   *
   * @param k
   *          a key
   * @param v
   *          a value
   * @return indicating whether to pass or block the key, and how far the source iterator should be advanced.
   */
  public abstract FilterResult filter(Key k, Value v);

  /**
   * Whenever the subclass returns AdvanceResult.USE_HINT from its filter predicate, this method will be called to see how far to advance the source iterator.
   * The return value must be a key which is greater than (sorts after) the input key. If the subclass never returns USE_HINT, this method will never be called
   * and may safely return null.
   *
   * @param k
   *          a key
   * @param v
   *          a value
   * @return as above
   */
  public abstract Key getNextKeyHint(Key k, Value v);

  private Collection<ByteSequence> columnFamilies;
  private boolean inclusive;
  private Range seekRange;
  private boolean negate;

  private AdvanceResult advance;

  private boolean advancedPastSeek = false;

  @Override
  public void next() throws IOException {
    advanceSource(getSource(), advance);
    findTop();
  }

  @Override
  public boolean hasTop() {
    return !advancedPastSeek && super.hasTop();
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    super.seek(range, columnFamilies, inclusive);
    advance = null;
    this.columnFamilies = columnFamilies;
    this.inclusive = inclusive;
    seekRange = range;
    advancedPastSeek = false;
    findTop();
  }

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    negate = Boolean.parseBoolean(options.get(NEGATE));
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
    newInstance.negate = negate;
    return newInstance;
  }

  protected void findTop() throws IOException {
    SortedKeyValueIterator<Key,Value> src = getSource();
    // advance could be null if we've just been seeked
    advance = null;
    while (src.hasTop() && !advancedPastSeek) {
      if (src.getTopKey().isDeleted()) {
        // as per. o.a.a.core.iterators.Filter, deleted keys always pass through the filter.
        advance = AdvanceResult.NEXT;
        return;
      }
      FilterResult f = filter(src.getTopKey(), src.getTopValue());
      if (log.isTraceEnabled()) {
        log.trace("Filtered: {} result == {} hint == {}", src.getTopKey(), f,
            f.advance == AdvanceResult.USE_HINT ? getNextKeyHint(src.getTopKey(), src.getTopValue()) : " (none)");
      }
      if (f.accept != negate) {
        // advance will be processed when next is called
        advance = f.advance;
        break;
      } else {
        advanceSource(src, f.advance);
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
        if (hintKey != null && hintKey.compareTo(topKey) > 0) {
          advRange = new Range(hintKey, null);
        } else {
          String msg = "Filter returned USE_HINT for " + topKey + " but invalid hint: " + hintKey;
          throw new IOException(msg);
        }
        break;
    }
    if (advRange == null) {
      // Should never get here. Just a safeguard in case somebody adds a new type of AdvanceRange and forgets to handle it here.
      throw new IOException("Unable to determine range to advance to for AdvanceResult " + adv);
    }
    advRange = advRange.clip(seekRange, true);
    if (advRange == null) {
      // the advanced range is outside the seek range. the source is exhausted.
      advancedPastSeek = true;
    } else {
      src.seek(advRange, columnFamilies, inclusive);
    }
  }
}
