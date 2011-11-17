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
package org.apache.accumulo.core.iterators;

import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.filter.NoLabelFilter;

/**
 * @deprecated since 1.4
 * @use org.apache.accumulo.core.iterators.user.NoVisFilter
 **/
public class NoLabelIterator extends SkippingIterator implements OptionDescriber {
  
  private NoLabelFilter ref = new NoLabelFilter();
  
  public NoLabelIterator deepCopy(IteratorEnvironment env) {
    return new NoLabelIterator(this, env);
  }
  
  private NoLabelIterator(NoLabelIterator other, IteratorEnvironment env) {
    setSource(other.getSource().deepCopy(env));
    ref = other.ref;
  }
  
  public NoLabelIterator() {}
  
  private boolean matches(Key key, Value value) {
    return ref.accept(key, value);
  }
  
  @Override
  protected void consume() throws IOException {
    while (getSource().hasTop() && !matches(getSource().getTopKey(), getSource().getTopValue())) {
      getSource().next();
    }
  }
  
  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    ref.init(options);
  }
  
  @Override
  public IteratorOptions describeOptions() {
    return ref.describeOptions();
  }
  
  @Override
  public boolean validateOptions(Map<String,String> options) {
    return ref.validateOptions(options);
  }
}
