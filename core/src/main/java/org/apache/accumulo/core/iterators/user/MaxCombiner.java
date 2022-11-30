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

import java.util.Iterator;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.iterators.LongCombiner;

/**
 * A Combiner that interprets Values as Longs and returns the largest Long among them.
 */
public class MaxCombiner extends LongCombiner {
  @Override
  public Long typedReduce(Key key, Iterator<Long> iter) {
    long max = Long.MIN_VALUE;
    while (iter.hasNext()) {
      Long l = iter.next();
      if (l > max) {
        max = l;
      }
    }
    return max;
  }

  @Override
  public IteratorOptions describeOptions() {
    IteratorOptions io = super.describeOptions();
    io.setName("max");
    io.setDescription("MaxCombiner interprets Values as Longs and finds their"
        + " maximum.  A variety of encodings (variable length, fixed length, or"
        + " string) are available");
    return io;
  }
}
