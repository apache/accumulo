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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;

import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.aggregation.Aggregator;
import org.apache.accumulo.examples.wikisearch.protobuf.TermWeight;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * An Aggregator to merge together a list of term offsets and one normalized term frequency
 * 
 */
public class TextIndexAggregator implements Aggregator {
  private static final Logger log = Logger.getLogger(TextIndexAggregator.class);
  
  private List<Integer> offsets = new ArrayList<Integer>();
  private TermWeight.Info.Builder builder = TermWeight.Info.newBuilder();
  private float normalizedTermFrequency = 0f;
  
  @Override
  public Value aggregate() {
    // Keep the sorted order we tried to maintain
    for (int i = 0; i < offsets.size(); ++i) {
      builder.addWordOffset(offsets.get(i));
    }
    
    builder.setNormalizedTermFrequency(normalizedTermFrequency);
    
    return new Value(builder.build().toByteArray());
  }
  
  @Override
  public void collect(Value value) {
    // Make sure we don't aggregate something else
    if (value == null || value.get().length == 0) {
      return;
    }
    
    TermWeight.Info info;
    
    try {
      info = TermWeight.Info.parseFrom(value.get());
    } catch (InvalidProtocolBufferException e) {
      log.error("Value passed to aggregator was not of type TermWeight.Info", e);
      return;
    }
    
    // Add each offset into the list maintaining sorted order
    for (int offset : info.getWordOffsetList()) {
      int pos = Collections.binarySearch(offsets, offset);
      
      if (pos < 0) {
        // Undo the transform on the insertion point
        offsets.add((-1 * pos) - 1, offset);
      } else {
        offsets.add(pos, offset);
      }
    }
    
    if (info.getNormalizedTermFrequency() > 0) {
      this.normalizedTermFrequency += info.getNormalizedTermFrequency();
    }
  }
  
  @Override
  public void reset() {
    this.offsets.clear();
    this.normalizedTermFrequency = 0f;
    this.builder = TermWeight.Info.newBuilder();
  }
  
}
