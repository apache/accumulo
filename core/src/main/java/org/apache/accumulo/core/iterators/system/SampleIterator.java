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

package org.apache.accumulo.core.iterators.system;

import org.apache.accumulo.core.client.sample.RowSampler;
import org.apache.accumulo.core.client.sample.Sampler;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

public class SampleIterator extends Filter {

  private Sampler sampler = new RowSampler();

  public SampleIterator(SortedKeyValueIterator<Key,Value> iter, Sampler sampler) {
    setSource(iter);
    this.sampler = sampler;
  }

  @Override
  public boolean accept(Key k, Value v) {
    return sampler.accept(k);
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    return new SampleIterator(getSource().deepCopy(env), sampler);
  }
}
