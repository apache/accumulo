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

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import java.io.IOException;
import java.util.Map;

public class CfCqSliceSeekingFilter extends SeekingFilter implements OptionDescriber {

  private static final FilterResult SKIP_TO_HINT = new FilterResult(false, AdvanceResult.USE_HINT);
  private static final FilterResult SKIP_TO_NEXT = new FilterResult(false, AdvanceResult.NEXT);
  private static final FilterResult SKIP_TO_NEXT_ROW = new FilterResult(false, AdvanceResult.NEXT_ROW);
  private static final FilterResult SKIP_TO_NEXT_CF = new FilterResult(false, AdvanceResult.NEXT_CF);
  private static final FilterResult INCLUDE_AND_NEXT = new FilterResult(true, AdvanceResult.NEXT);
  private static final FilterResult INCLUDE_AND_NEXT_CF = new FilterResult(true, AdvanceResult.NEXT_CF);

  private CfCqSliceOpts cso;

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env)
          throws IOException {
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
        // special-case here: we know we're at the last CQ in the slice, so skip to the next CF in the row.
        return INCLUDE_AND_NEXT_CF;
      }
    }
    // at this point we're in the CQ slice.
    return INCLUDE_AND_NEXT;
  }

  @Override
  public Key getNextKeyHint(Key k, Value v) {
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
        return cso.minInclusive ? hint: hint.followingKey(PartialKey.ROW_COLFAM_COLQUAL);
      }
    }
    return null;
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    CfCqSliceSeekingFilter o = (CfCqSliceSeekingFilter) super.deepCopy(env);
    o.cso = new CfCqSliceOpts(cso);
    return o;
  }

  @Override
  public IteratorOptions describeOptions() {
    return cso.describeOptions();
  }

  @Override
  public boolean validateOptions(Map<String,String> options) {
    return cso.validateOptions(options);
  }
}
