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
import java.util.List;
import java.util.UUID;

import junit.framework.TestCase;

import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.aggregation.Aggregator;
import org.apache.accumulo.examples.wikisearch.aggregator.GlobalIndexUidAggregator;
import org.apache.accumulo.examples.wikisearch.protobuf.Uid;
import org.apache.accumulo.examples.wikisearch.protobuf.Uid.List.Builder;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


@SuppressWarnings("deprecation")
public class GlobalIndexUidAggregatorTest extends TestCase {
  
  Aggregator agg = new GlobalIndexUidAggregator();
  
  private Uid.List.Builder createNewUidList() {
    return Uid.List.newBuilder();
  }
  
  public void testSingleUid() {
    agg.reset();
    Builder b = createNewUidList();
    b.setCOUNT(1);
    b.setIGNORE(false);
    b.addUID(UUID.randomUUID().toString());
    Uid.List uidList = b.build();
    Value val = new Value(uidList.toByteArray());
    agg.collect(val);
    Value result = agg.aggregate();
    assertTrue(val.compareTo(result.get()) == 0);
  }
  
  public void testLessThanMax() throws Exception {
    agg.reset();
    List<String> savedUUIDs = new ArrayList<String>();
    for (int i = 0; i < GlobalIndexUidAggregator.MAX - 1; i++) {
      Builder b = createNewUidList();
      b.setIGNORE(false);
      String uuid = UUID.randomUUID().toString();
      savedUUIDs.add(uuid);
      b.setCOUNT(i);
      b.addUID(uuid);
      Uid.List uidList = b.build();
      Value val = new Value(uidList.toByteArray());
      agg.collect(val);
    }
    Value result = agg.aggregate();
    Uid.List resultList = Uid.List.parseFrom(result.get());
    assertTrue(resultList.getIGNORE() == false);
    assertTrue(resultList.getUIDCount() == (GlobalIndexUidAggregator.MAX - 1));
    List<String> resultListUUIDs = resultList.getUIDList();
    for (String s : savedUUIDs)
      assertTrue(resultListUUIDs.contains(s));
  }
  
  public void testEqualsMax() throws Exception {
    agg.reset();
    List<String> savedUUIDs = new ArrayList<String>();
    for (int i = 0; i < GlobalIndexUidAggregator.MAX; i++) {
      Builder b = createNewUidList();
      b.setIGNORE(false);
      String uuid = UUID.randomUUID().toString();
      savedUUIDs.add(uuid);
      b.setCOUNT(i);
      b.addUID(uuid);
      Uid.List uidList = b.build();
      Value val = new Value(uidList.toByteArray());
      agg.collect(val);
    }
    Value result = agg.aggregate();
    Uid.List resultList = Uid.List.parseFrom(result.get());
    assertTrue(resultList.getIGNORE() == false);
    assertTrue(resultList.getUIDCount() == (GlobalIndexUidAggregator.MAX));
    List<String> resultListUUIDs = resultList.getUIDList();
    for (String s : savedUUIDs)
      assertTrue(resultListUUIDs.contains(s));
  }
  
  public void testMoreThanMax() throws Exception {
    agg.reset();
    List<String> savedUUIDs = new ArrayList<String>();
    for (int i = 0; i < GlobalIndexUidAggregator.MAX + 10; i++) {
      Builder b = createNewUidList();
      b.setIGNORE(false);
      String uuid = UUID.randomUUID().toString();
      savedUUIDs.add(uuid);
      b.setCOUNT(1);
      b.addUID(uuid);
      Uid.List uidList = b.build();
      Value val = new Value(uidList.toByteArray());
      agg.collect(val);
    }
    Value result = agg.aggregate();
    Uid.List resultList = Uid.List.parseFrom(result.get());
    assertTrue(resultList.getIGNORE() == true);
    assertTrue(resultList.getUIDCount() == 0);
    assertTrue(resultList.getCOUNT() == (GlobalIndexUidAggregator.MAX + 10));
  }
  
  public void testSeenIgnore() throws Exception {
    agg.reset();
    Builder b = createNewUidList();
    b.setIGNORE(true);
    b.setCOUNT(0);
    Uid.List uidList = b.build();
    Value val = new Value(uidList.toByteArray());
    agg.collect(val);
    b = createNewUidList();
    b.setIGNORE(false);
    b.setCOUNT(1);
    b.addUID(UUID.randomUUID().toString());
    uidList = b.build();
    val = new Value(uidList.toByteArray());
    agg.collect(val);
    Value result = agg.aggregate();
    Uid.List resultList = Uid.List.parseFrom(result.get());
    assertTrue(resultList.getIGNORE() == true);
    assertTrue(resultList.getUIDCount() == 0);
    assertTrue(resultList.getCOUNT() == 1);
  }
  
  public void testInvalidValueType() throws Exception {
    Logger.getLogger(GlobalIndexUidAggregator.class).setLevel(Level.OFF);
    agg.reset();
    Value val = new Value(UUID.randomUUID().toString().getBytes());
    agg.collect(val);
    Value result = agg.aggregate();
    Uid.List resultList = Uid.List.parseFrom(result.get());
    assertTrue(resultList.getIGNORE() == false);
    assertTrue(resultList.getUIDCount() == 0);
    assertTrue(resultList.getCOUNT() == 0);
  }
  
  public void testCount() throws Exception {
    agg.reset();
    UUID uuid = UUID.randomUUID();
    // Collect the same UUID five times.
    for (int i = 0; i < 5; i++) {
      Builder b = createNewUidList();
      b.setCOUNT(1);
      b.setIGNORE(false);
      b.addUID(uuid.toString());
      Uid.List uidList = b.build();
      Value val = new Value(uidList.toByteArray());
      agg.collect(val);
    }
    Value result = agg.aggregate();
    Uid.List resultList = Uid.List.parseFrom(result.get());
    assertTrue(resultList.getIGNORE() == false);
    assertTrue(resultList.getUIDCount() == 1);
    assertTrue(resultList.getCOUNT() == 5);
    
  }
  
}
