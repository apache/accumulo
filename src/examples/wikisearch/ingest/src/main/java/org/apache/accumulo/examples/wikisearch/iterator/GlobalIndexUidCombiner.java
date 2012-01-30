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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.TypedValueCombiner;
import org.apache.accumulo.core.iterators.ValueFormatException;
import org.apache.accumulo.examples.wikisearch.protobuf.Uid;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * 
 */
public class GlobalIndexUidCombiner extends TypedValueCombiner<Uid.List> {
  public static final Encoder<Uid.List> UID_LIST_ENCODER = new UidListEncoder();
  public static final int MAX = 20;
  
  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    setEncoder(UID_LIST_ENCODER);
  }
  
  @Override
  public Uid.List typedReduce(Key key, Iterator<Uid.List> iter) {
    Uid.List.Builder builder = Uid.List.newBuilder();
    HashSet<String> uids = new HashSet<String>();
    boolean seenIgnore = false;
    long count = 0;
    while (iter.hasNext()) {
      Uid.List v = iter.next();
      if (null == v)
        continue;
      count = count + v.getCOUNT();
      if (v.getIGNORE()) {
        seenIgnore = true;
      }
      uids.addAll(v.getUIDList());
    }
    // Special case logic
    // If we have aggregated more than MAX UIDs, then null out the UID list and set IGNORE to true
    // However, always maintain the count
    builder.setCOUNT(count);
    if (uids.size() > MAX || seenIgnore) {
      builder.setIGNORE(true);
      builder.clearUID();
    } else {
      builder.setIGNORE(false);
      builder.addAllUID(uids);
    }
    return builder.build();
  }
  
  public static class UidListEncoder implements Encoder<Uid.List> {
    @Override
    public byte[] encode(Uid.List v) {
      return v.toByteArray();
    }
    
    @Override
    public Uid.List decode(byte[] b) {
      if (b.length == 0)
        return null;
      try {
        return Uid.List.parseFrom(b);
      } catch (InvalidProtocolBufferException e) {
        throw new ValueFormatException("Value passed to aggregator was not of type Uid.List");
      }
    }
  }
}
