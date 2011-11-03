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
package org.apache.accumulo.examples.aggregation;

import java.util.TreeSet;

import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.aggregation.Aggregator;
import org.apache.accumulo.core.util.StringUtil;

public class SortedSetAggregator implements Aggregator {
  
  TreeSet<String> items = new TreeSet<String>();
  
  // aggregate the entire set of items, in sorted order
  public Value aggregate() {
    return new Value(StringUtil.join(items, ",").getBytes());
  }
  
  // allow addition of multiple items at a time to the set
  public void collect(Value value) {
    String[] strings = value.toString().split(",");
    for (String s : strings)
      items.add(s);
  }
  
  public void reset() {
    items.clear();
  }
  
}
