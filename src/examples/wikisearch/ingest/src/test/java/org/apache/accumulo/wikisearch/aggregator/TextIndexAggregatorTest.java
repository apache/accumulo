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
package org.apache.accumulo.wikisearch.aggregator;

import java.util.List;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.wikisearch.aggregator.TextIndexAggregator;
import org.apache.accumulo.wikisearch.protobuf.TermWeight;
import org.apache.accumulo.wikisearch.protobuf.TermWeight.Info.Builder;

import com.google.protobuf.InvalidProtocolBufferException;

public class TextIndexAggregatorTest {
  private TextIndexAggregator aggregator;
  
  @Before
  public void setup() throws Exception {
    aggregator = new TextIndexAggregator();
  }
  
  @After
  public void cleanup() {
    
  }
  
  private TermWeight.Info.Builder createBuilder() {
    return TermWeight.Info.newBuilder();
  }
  
  @Test
  public void testSingleValue() throws InvalidProtocolBufferException {
    aggregator = new TextIndexAggregator();
    Builder builder = createBuilder();
    builder.addWordOffset(1);
    builder.addWordOffset(5);
    builder.setNormalizedTermFrequency(0.1f);
    
    aggregator.collect(new Value(builder.build().toByteArray()));
    
    Value result = aggregator.aggregate();
    
    TermWeight.Info info = TermWeight.Info.parseFrom(result.get());
    
    Assert.assertTrue(info.getNormalizedTermFrequency() == 0.1f);
    
    List<Integer> offsets = info.getWordOffsetList();
    Assert.assertTrue(offsets.size() == 2);
    Assert.assertTrue(offsets.get(0) == 1);
    Assert.assertTrue(offsets.get(1) == 5);
  }
  
  @Test
  public void testAggregateTwoValues() throws InvalidProtocolBufferException {
    aggregator = new TextIndexAggregator();
    Builder builder = createBuilder();
    builder.addWordOffset(1);
    builder.addWordOffset(5);
    builder.setNormalizedTermFrequency(0.1f);
    
    aggregator.collect(new Value(builder.build().toByteArray()));
    
    builder = createBuilder();
    builder.addWordOffset(3);
    builder.setNormalizedTermFrequency(0.05f);
    
    aggregator.collect(new Value(builder.build().toByteArray()));
    
    Value result = aggregator.aggregate();
    
    TermWeight.Info info = TermWeight.Info.parseFrom(result.get());
    
    Assert.assertTrue(info.getNormalizedTermFrequency() == 0.15f);
    
    List<Integer> offsets = info.getWordOffsetList();
    Assert.assertTrue(offsets.size() == 3);
    Assert.assertTrue(offsets.get(0) == 1);
    Assert.assertTrue(offsets.get(1) == 3);
    Assert.assertTrue(offsets.get(2) == 5);
  }
  
  @Test
  public void testAggregateManyValues() throws InvalidProtocolBufferException {
    aggregator = new TextIndexAggregator();
    
    Builder builder = createBuilder();
    builder.addWordOffset(13);
    builder.addWordOffset(15);
    builder.addWordOffset(19);
    builder.setNormalizedTermFrequency(0.12f);
    
    aggregator.collect(new Value(builder.build().toByteArray()));
    
    builder = createBuilder();
    builder.addWordOffset(1);
    builder.addWordOffset(5);
    builder.setNormalizedTermFrequency(0.1f);
    
    aggregator.collect(new Value(builder.build().toByteArray()));
    
    builder = createBuilder();
    builder.addWordOffset(3);
    builder.setNormalizedTermFrequency(0.05f);
    
    aggregator.collect(new Value(builder.build().toByteArray()));
    
    Value result = aggregator.aggregate();
    
    TermWeight.Info info = TermWeight.Info.parseFrom(result.get());
    
    Assert.assertTrue(info.getNormalizedTermFrequency() == 0.27f);
    
    List<Integer> offsets = info.getWordOffsetList();
    Assert.assertTrue(offsets.size() == 6);
    Assert.assertTrue(offsets.get(0) == 1);
    Assert.assertTrue(offsets.get(1) == 3);
    Assert.assertTrue(offsets.get(2) == 5);
    Assert.assertTrue(offsets.get(3) == 13);
    Assert.assertTrue(offsets.get(4) == 15);
    Assert.assertTrue(offsets.get(5) == 19);
  }
}
