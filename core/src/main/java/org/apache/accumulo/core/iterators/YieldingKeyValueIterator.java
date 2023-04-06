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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * An iterator that supports yielding on a next or seek call (only used by SortedKeyValueIterators)
 */
public interface YieldingKeyValueIterator<K extends WritableComparable<?>,V extends Writable> {

  /**
   * Allows implementations to preempt further iteration of this iterator in the current RPC.
   * Implementations can use the yield method on the callback to instruct the caller to cease
   * collecting more results within this RPC. An implementation would only need to implement this
   * mechanism if a next or seek call has been taking so long as to starve out other scans within
   * the same thread pool. Most iterators do not need to implement this method. The yield method on
   * the callback accepts a Key which will be used as the start key (non-inclusive) on the seek call
   * in the next RPC. This feature is not supported for isolated scans.
   */
  default void enableYielding(YieldCallback<K> callback) {
    // noop
  }

}
