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
package org.apache.accumulo.core.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.util.LocalityGroupUtil.Partitioner;
import org.apache.commons.lang.mutable.MutableLong;
import org.junit.Assert;
import org.junit.Test;

public class PartitionerTest {
  @Test
  public void test1() {

    @SuppressWarnings("unchecked")
    Map<ByteSequence,MutableLong>[] groups = new Map[2];

    groups[0] = new HashMap<ByteSequence,MutableLong>();
    groups[0].put(new ArrayByteSequence("cf1"), new MutableLong(1));
    groups[0].put(new ArrayByteSequence("cf2"), new MutableLong(1));

    groups[1] = new HashMap<ByteSequence,MutableLong>();
    groups[1].put(new ArrayByteSequence("cf3"), new MutableLong(1));

    Partitioner p1 = new Partitioner(groups);

    Mutation m1 = new Mutation("r1");
    m1.put("cf1", "cq1", "v1");

    Mutation m2 = new Mutation("r2");
    m2.put("cf1", "cq1", "v2");
    m2.put("cf2", "cq2", "v3");

    Mutation m3 = new Mutation("r3");
    m3.put("cf1", "cq1", "v4");
    m3.put("cf3", "cq2", "v5");

    Mutation m4 = new Mutation("r4");
    m4.put("cf1", "cq1", "v6");
    m4.put("cf3", "cq2", "v7");
    m4.put("cf5", "cq3", "v8");

    Mutation m5 = new Mutation("r5");
    m5.put("cf5", "cq3", "v9");

    List<Mutation> mutations = Arrays.asList(m1, m2, m3, m4, m5);
    @SuppressWarnings("unchecked")
    List<Mutation>[] partitioned = new List[3];

    for (int i = 0; i < partitioned.length; i++) {
      partitioned[i] = new ArrayList<Mutation>();
    }

    p1.partition(mutations, partitioned);

    m1 = new Mutation("r1");
    m1.put("cf1", "cq1", "v1");

    m2 = new Mutation("r2");
    m2.put("cf1", "cq1", "v2");
    m2.put("cf2", "cq2", "v3");

    m3 = new Mutation("r3");
    m3.put("cf1", "cq1", "v4");

    m4 = new Mutation("r4");
    m4.put("cf1", "cq1", "v6");

    Assert.assertEquals(toKeySet(m1, m2, m3, m4), toKeySet(partitioned[0]));

    m3 = new Mutation("r3");
    m3.put("cf3", "cq2", "v5");

    m4 = new Mutation("r4");
    m4.put("cf3", "cq2", "v7");

    Assert.assertEquals(toKeySet(m3, m4), toKeySet(partitioned[1]));

    m4 = new Mutation("r4");
    m4.put("cf5", "cq3", "v8");

    Assert.assertEquals(toKeySet(m4, m5), toKeySet(partitioned[2]));

  }

  private Set<Key> toKeySet(List<Mutation> mutations) {
    return toKeySet(mutations.toArray(new Mutation[0]));
  }

  private Set<Key> toKeySet(Mutation... expected) {
    HashSet<Key> ret = new HashSet<Key>();
    for (Mutation mutation : expected)
      for (ColumnUpdate cu : mutation.getUpdates())
        ret.add(new Key(mutation.getRow(), cu.getColumnFamily(), cu.getColumnQualifier(), cu.getColumnVisibility(), cu.getTimestamp()));

    return ret;
  }
}
