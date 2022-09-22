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
package org.apache.accumulo.core.iterators;

import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

public class SortedKeyIterator extends WrappingIterator implements OptionDescriber {
  private static final Value NOVALUE = new Value();

  public SortedKeyIterator() {}

  public SortedKeyIterator(SortedKeyIterator other, IteratorEnvironment env) {
    setSource(other.getSource().deepCopy(env));
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    return new SortedKeyIterator(this, env);
  }

  @Override
  public Value getTopValue() {
    return NOVALUE;
  }

  @Override
  public IteratorOptions describeOptions() {
    return new IteratorOptions("keyset",
        SortedKeyIterator.class.getSimpleName() + " filters out values, but leaves keys intact",
        null, null);
  }

  @Override
  public boolean validateOptions(Map<String,String> options) {
    return true;
  }
}
