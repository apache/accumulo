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
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

/**
 * Filters key/value pairs for a range of column families and a range of column qualifiers. Only
 * keys which fall in both ranges will be passed by the filter. Note that if you have a small,
 * well-defined set of column families it will be much more efficient to configure locality groups
 * to isolate that data instead of configuring this iterator to scan over it.
 *
 * @see org.apache.accumulo.core.iterators.user.CfCqSliceOpts for a description of this iterator's
 *      options.
 */
public class CfCqSliceFilter extends Filter {

  private CfCqSliceOpts cso;

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    cso = new CfCqSliceOpts(options);
  }

  @Override
  public boolean accept(Key k, Value v) {
    PartialKey inSlice = isKeyInSlice(k);
    return inSlice == PartialKey.ROW_COLFAM_COLQUAL;
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    CfCqSliceFilter o = (CfCqSliceFilter) super.deepCopy(env);
    o.cso = new CfCqSliceOpts(cso);
    return o;
  }

  private PartialKey isKeyInSlice(Key k) {
    if (cso.minCf.getLength() > 0) {
      int minCfComp = k.compareColumnFamily(cso.minCf);
      if (minCfComp < 0 || (minCfComp == 0 && !cso.minInclusive)) {
        return PartialKey.ROW;
      }
    }
    if (cso.maxCf.getLength() > 0) {
      int maxCfComp = k.compareColumnFamily(cso.maxCf);
      if (maxCfComp > 0 || (maxCfComp == 0 && !cso.maxInclusive)) {
        return PartialKey.ROW;
      }
    }
    // k.colfam is in the "slice".
    if (cso.minCq.getLength() > 0) {
      int minCqComp = k.compareColumnQualifier(cso.minCq);
      if (minCqComp < 0 || (minCqComp == 0 && !cso.minInclusive)) {
        return PartialKey.ROW_COLFAM;
      }
    }
    if (cso.maxCq.getLength() > 0) {
      int maxCqComp = k.compareColumnQualifier(cso.maxCq);
      if (maxCqComp > 0 || (maxCqComp == 0 && !cso.maxInclusive)) {
        return PartialKey.ROW_COLFAM;
      }
    }
    // k.colqual is in the slice.
    return PartialKey.ROW_COLFAM_COLQUAL;
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
