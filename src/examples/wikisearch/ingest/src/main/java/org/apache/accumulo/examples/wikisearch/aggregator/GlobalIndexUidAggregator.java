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
package org.apache.accumulo.examples.wikisearch.aggregator;

import java.util.HashSet;

import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.aggregation.Aggregator;
import org.apache.accumulo.examples.wikisearch.protobuf.Uid;
import org.apache.log4j.Logger;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Implementation of an Aggregator that aggregates objects of the type Uid.List. This is an optimization for the global index and global reverse index, where
 * the list of UIDs for events will be maintained in the index for low cardinality terms (Low in this case being less than 20).
 * 
 */
public class GlobalIndexUidAggregator implements Aggregator {
  
  private static final Logger log = Logger.getLogger(GlobalIndexUidAggregator.class);
  private Uid.List.Builder builder = Uid.List.newBuilder();
  // Using a set instead of a list so that duplicate IDs are filtered out of the list.
  private HashSet<String> uids = new HashSet<String>();
  private boolean seenIgnore = false;
  public static final int MAX = 20;
  private long count = 0;
  
  @Override
  public Value aggregate() {
    // Special case logic
    // If we have aggregated more than MAX UIDs, then null out the UID list and set IGNORE to true
    // However, always maintain the count
    if (uids.size() > MAX || seenIgnore) {
      builder.setCOUNT(count);
      builder.setIGNORE(true);
      builder.clearUID();
    } else {
      builder.setCOUNT(count);
      builder.setIGNORE(false);
      builder.addAllUID(uids);
    }
    return new Value(builder.build().toByteArray());
  }
  
  @Override
  public void collect(Value value) {
    if (null == value || value.get().length == 0)
      return;
    // Collect the values, which are serialized Uid.List objects
    try {
      Uid.List v = Uid.List.parseFrom(value.get());
      count = count + v.getCOUNT();
      if (v.getIGNORE()) {
        seenIgnore = true;
      }
      // Add the incoming list to this list
      uids.addAll(v.getUIDList());
    } catch (InvalidProtocolBufferException e) {
      log.error("Value passed to aggregator was not of type Uid.List", e);
    }
  }
  
  @Override
  public void reset() {
    count = 0;
    seenIgnore = false;
    builder = Uid.List.newBuilder();
    uids.clear();
  }
  
}
