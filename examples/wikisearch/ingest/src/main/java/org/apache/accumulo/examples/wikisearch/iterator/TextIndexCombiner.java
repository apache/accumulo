/**
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
package org.apache.accumulo.examples.wikisearch.iterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.TypedValueCombiner;
import org.apache.accumulo.core.iterators.ValueFormatException;
import org.apache.accumulo.examples.wikisearch.protobuf.TermWeight;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * 
 */
public class TextIndexCombiner extends TypedValueCombiner<TermWeight.Info> {
  public static final Encoder<TermWeight.Info> TERMWEIGHT_INFO_ENCODER = new TermWeightInfoEncoder();
  
  @Override
  public TermWeight.Info typedReduce(Key key, Iterator<TermWeight.Info> iter) {
    TermWeight.Info.Builder builder = TermWeight.Info.newBuilder();
    List<Integer> offsets = new ArrayList<Integer>();
    float normalizedTermFrequency = 0f;
    
    while (iter.hasNext()) {
      TermWeight.Info info = iter.next();
      if (null == info)
        continue;
      
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
        normalizedTermFrequency += info.getNormalizedTermFrequency();
      }
    }
    
    // Keep the sorted order we tried to maintain
    for (int i = 0; i < offsets.size(); ++i) {
      builder.addWordOffset(offsets.get(i));
    }
    
    builder.setNormalizedTermFrequency(normalizedTermFrequency);
    return builder.build();
  }
  
  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    setEncoder(TERMWEIGHT_INFO_ENCODER);
  }
  
  public static class TermWeightInfoEncoder implements Encoder<TermWeight.Info> {
    @Override
    public byte[] encode(TermWeight.Info v) {
      return v.toByteArray();
    }
    
    @Override
    public TermWeight.Info decode(byte[] b) {
      if (b.length == 0)
        return null;
      try {
        return TermWeight.Info.parseFrom(b);
      } catch (InvalidProtocolBufferException e) {
        throw new ValueFormatException("Value passed to aggregator was not of type TermWeight.Info");
      }
    }
  }
}
