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
package org.apache.accumulo.core.iterators.aggregation;

import org.apache.accumulo.core.data.Value;

/**
 * @deprecated since 1.4, replaced by {@link org.apache.accumulo.core.iterators.user.MinCombiner} with
 *             {@link org.apache.accumulo.core.iterators.LongCombiner.Type#STRING}
 */
@Deprecated
public class StringMin implements Aggregator {

  long min = Long.MAX_VALUE;

  @Override
  public Value aggregate() {
    return new Value(Long.toString(min).getBytes());
  }

  @Override
  public void collect(Value value) {
    long l = Long.parseLong(new String(value.get()));
    if (l < min) {
      min = l;
    }
  }

  @Override
  public void reset() {
    min = Long.MAX_VALUE;
  }

}
