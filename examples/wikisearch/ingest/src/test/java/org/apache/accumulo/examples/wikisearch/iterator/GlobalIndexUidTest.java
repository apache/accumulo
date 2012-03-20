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
package org.apache.accumulo.examples.wikisearch.iterator;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.examples.wikisearch.protobuf.Uid;
import org.apache.accumulo.examples.wikisearch.protobuf.Uid.List.Builder;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

public class GlobalIndexUidTest {
  private GlobalIndexUidCombiner combiner;
  private List<Value> values;
  
  @Before
  public void setup() throws Exception {
    combiner = new GlobalIndexUidCombiner();
    combiner.init(null, Collections.singletonMap("all", "true"), null);
    values = new ArrayList<Value>();
  }
  
  private Uid.List.Builder createNewUidList() {
    return Uid.List.newBuilder();
  }
  
  @Test
  public void testSingleUid() {
    Builder b = createNewUidList();
    b.setCOUNT(1);
    b.setIGNORE(false);
    b.addUID(UUID.randomUUID().toString());
    Uid.List uidList = b.build();
    Value val = new Value(uidList.toByteArray());
    values.add(val);
    Value result = combiner.reduce(new Key(), values.iterator());
    assertTrue(val.compareTo(result.get()) == 0);
  }
  
  @Test
  public void testLessThanMax() throws Exception {
    List<String> savedUUIDs = new ArrayList<String>();
    for (int i = 0; i < GlobalIndexUidCombiner.MAX - 1; i++) {
      Builder b = createNewUidList();
      b.setIGNORE(false);
      String uuid = UUID.randomUUID().toString();
      savedUUIDs.add(uuid);
      b.setCOUNT(i);
      b.addUID(uuid);
      Uid.List uidList = b.build();
      Value val = new Value(uidList.toByteArray());
      values.add(val);
    }
    Value result = combiner.reduce(new Key(), values.iterator());
    Uid.List resultList = Uid.List.parseFrom(result.get());
    assertTrue(resultList.getIGNORE() == false);
    assertTrue(resultList.getUIDCount() == (GlobalIndexUidCombiner.MAX - 1));
    List<String> resultListUUIDs = resultList.getUIDList();
    for (String s : savedUUIDs)
      assertTrue(resultListUUIDs.contains(s));
  }
  
  @Test
  public void testEqualsMax() throws Exception {
    List<String> savedUUIDs = new ArrayList<String>();
    for (int i = 0; i < GlobalIndexUidCombiner.MAX; i++) {
      Builder b = createNewUidList();
      b.setIGNORE(false);
      String uuid = UUID.randomUUID().toString();
      savedUUIDs.add(uuid);
      b.setCOUNT(i);
      b.addUID(uuid);
      Uid.List uidList = b.build();
      Value val = new Value(uidList.toByteArray());
      values.add(val);
    }
    Value result = combiner.reduce(new Key(), values.iterator());
    Uid.List resultList = Uid.List.parseFrom(result.get());
    assertTrue(resultList.getIGNORE() == false);
    assertTrue(resultList.getUIDCount() == (GlobalIndexUidCombiner.MAX));
    List<String> resultListUUIDs = resultList.getUIDList();
    for (String s : savedUUIDs)
      assertTrue(resultListUUIDs.contains(s));
  }
  
  @Test
  public void testMoreThanMax() throws Exception {
    List<String> savedUUIDs = new ArrayList<String>();
    for (int i = 0; i < GlobalIndexUidCombiner.MAX + 10; i++) {
      Builder b = createNewUidList();
      b.setIGNORE(false);
      String uuid = UUID.randomUUID().toString();
      savedUUIDs.add(uuid);
      b.setCOUNT(1);
      b.addUID(uuid);
      Uid.List uidList = b.build();
      Value val = new Value(uidList.toByteArray());
      values.add(val);
    }
    Value result = combiner.reduce(new Key(), values.iterator());
    Uid.List resultList = Uid.List.parseFrom(result.get());
    assertTrue(resultList.getIGNORE() == true);
    assertTrue(resultList.getUIDCount() == 0);
    assertTrue(resultList.getCOUNT() == (GlobalIndexUidCombiner.MAX + 10));
  }
  
  @Test
  public void testSeenIgnore() throws Exception {
    Builder b = createNewUidList();
    b.setIGNORE(true);
    b.setCOUNT(0);
    Uid.List uidList = b.build();
    Value val = new Value(uidList.toByteArray());
    values.add(val);
    b = createNewUidList();
    b.setIGNORE(false);
    b.setCOUNT(1);
    b.addUID(UUID.randomUUID().toString());
    uidList = b.build();
    val = new Value(uidList.toByteArray());
    values.add(val);
    Value result = combiner.reduce(new Key(), values.iterator());
    Uid.List resultList = Uid.List.parseFrom(result.get());
    assertTrue(resultList.getIGNORE() == true);
    assertTrue(resultList.getUIDCount() == 0);
    assertTrue(resultList.getCOUNT() == 1);
  }
  
  @Test
  public void testInvalidValueType() throws Exception {
    Combiner comb = new GlobalIndexUidCombiner();
    IteratorSetting setting = new IteratorSetting(1, GlobalIndexUidCombiner.class);
    GlobalIndexUidCombiner.setCombineAllColumns(setting, true);
    GlobalIndexUidCombiner.setLossyness(setting, true);
    comb.init(null, setting.getOptions(), null);
    Logger.getLogger(GlobalIndexUidCombiner.class).setLevel(Level.OFF);
    Value val = new Value(UUID.randomUUID().toString().getBytes());
    values.add(val);
    Value result = comb.reduce(new Key(), values.iterator());
    Uid.List resultList = Uid.List.parseFrom(result.get());
    assertTrue(resultList.getIGNORE() == false);
    assertTrue(resultList.getUIDCount() == 0);
    assertTrue(resultList.getCOUNT() == 0);
  }
  
  @Test
  public void testCount() throws Exception {
    UUID uuid = UUID.randomUUID();
    // Collect the same UUID five times.
    for (int i = 0; i < 5; i++) {
      Builder b = createNewUidList();
      b.setCOUNT(1);
      b.setIGNORE(false);
      b.addUID(uuid.toString());
      Uid.List uidList = b.build();
      Value val = new Value(uidList.toByteArray());
      values.add(val);
    }
    Value result = combiner.reduce(new Key(), values.iterator());
    Uid.List resultList = Uid.List.parseFrom(result.get());
    assertTrue(resultList.getIGNORE() == false);
    assertTrue(resultList.getUIDCount() == 1);
    assertTrue(resultList.getCOUNT() == 5);
    
  }
  
}
