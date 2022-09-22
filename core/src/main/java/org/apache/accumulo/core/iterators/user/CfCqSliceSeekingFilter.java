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
package org.apache.accumulo.core.iterators.user;

import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

/**
 * Filters key/value pairs for a range of column families and a range of column qualifiers. Only
 * keys which fall in both ranges will be passed by the filter. Note that if you have a small,
 * well-defined set of column families it will be much more efficient to configure locality groups
 * to isolate that data instead of configuring this iterator to seek over it.
 *
 * This filter may be more efficient than the CfCqSliceFilter or the ColumnSlice filter for small
 * slices of large rows as it will seek to the next potential match once it determines that it has
 * iterated past the end of a slice.
 *
 * @see org.apache.accumulo.core.iterators.user.CfCqSliceOpts for a description of this iterator's
 *      options.
 */
public class CfCqSliceSeekingFilter extends SeekingFilter implements OptionDescriber {

  private static final FilterResult SKIP_TO_HINT = FilterResult.of(false, AdvanceResult.USE_HINT);
  private static final FilterResult SKIP_TO_NEXT = FilterResult.of(false, AdvanceResult.NEXT);
  private static final FilterResult SKIP_TO_NEXT_ROW =
      FilterResult.of(false, AdvanceResult.NEXT_ROW);
  private static final FilterResult SKIP_TO_NEXT_CF = FilterResult.of(false, AdvanceResult.NEXT_CF);
  private static final FilterResult INCLUDE_AND_NEXT = FilterResult.of(true, AdvanceResult.NEXT);
  private static final FilterResult INCLUDE_AND_NEXT_CF =
      FilterResult.of(true, AdvanceResult.NEXT_CF);

  private CfCqSliceOpts cso;

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    cso = new CfCqSliceOpts(options);
  }

  @Override
  public FilterResult filter(Key k, Value v) {
    if (cso.minCf.getLength() > 0) {
      int minCfCmp = k.compareColumnFamily(cso.minCf);
      if (minCfCmp < 0) {
        return SKIP_TO_HINT; // hint will be the min CF in this row.
      }
      if (minCfCmp == 0 && !cso.minInclusive) {
        return SKIP_TO_NEXT;
      }
    }
    if (cso.maxCf.getLength() > 0) {
      int maxCfCmp = k.compareColumnFamily(cso.maxCf);
      if (maxCfCmp > 0 || (maxCfCmp == 0 && !cso.maxInclusive)) {
        return SKIP_TO_NEXT_ROW;
      }
    }
    // at this point we're in the correct CF range, now check the CQ.
    if (cso.minCq.getLength() > 0) {
      int minCqCmp = k.compareColumnQualifier(cso.minCq);
      if (minCqCmp < 0) {
        return SKIP_TO_HINT; // hint will be the min CQ in this CF in this row.
      }
      if (minCqCmp == 0 && !cso.minInclusive) {
        return SKIP_TO_NEXT;
      }
    }
    if (cso.maxCq.getLength() > 0) {
      int maxCqCmp = k.compareColumnQualifier(cso.maxCq);
      if (maxCqCmp > 0 || (maxCqCmp == 0 && !cso.maxInclusive)) {
        return SKIP_TO_NEXT_CF;
      }
      if (maxCqCmp == 0) {
        // special-case here: we know we're at the last CQ in the slice, so skip to the next CF in
        // the row.
        return INCLUDE_AND_NEXT_CF;
      }
    }
    // at this point we're in the CQ slice.
    return INCLUDE_AND_NEXT;
  }

  @Override
  public Key getNextKeyHint(Key k, Value v) throws IllegalArgumentException {
    if (cso.minCf.getLength() > 0) {
      int minCfCmp = k.compareColumnFamily(cso.minCf);
      if (minCfCmp < 0) {
        Key hint = new Key(k.getRow(), cso.minCf);
        return cso.minInclusive ? hint : hint.followingKey(PartialKey.ROW_COLFAM);
      }
    }
    if (cso.minCq.getLength() > 0) {
      int minCqCmp = k.compareColumnQualifier(cso.minCq);
      if (minCqCmp < 0) {
        Key hint = new Key(k.getRow(), k.getColumnFamily(), cso.minCq);
        return cso.minInclusive ? hint : hint.followingKey(PartialKey.ROW_COLFAM_COLQUAL);
      }
    }
    // If we get here it means that we were asked to provide a hint for a key that we
    // didn't return USE_HINT for.
    throw new IllegalArgumentException("Don't know how to provide hint for key " + k);
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    CfCqSliceSeekingFilter o = (CfCqSliceSeekingFilter) super.deepCopy(env);
    o.cso = new CfCqSliceOpts(cso);
    return o;
  }

  @Override
  public IteratorOptions describeOptions() {
    return new CfCqSliceOpts.Describer().describeOptions();
  }

  @Override
  public boolean validateOptions(Map<String,String> options) {
    return new CfCqSliceOpts.Describer().validateOptions(options);
  }
}
