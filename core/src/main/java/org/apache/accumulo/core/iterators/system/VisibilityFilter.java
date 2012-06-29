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

import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.Filterer;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.predicates.ColumnVisibilityPredicate;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.commons.collections.map.LRUMap;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

public class VisibilityFilter extends Filter {
  private Authorizations auths;
  private Text defaultVisibility;
  private LRUMap cache;
  private Text tmpVis;
  
  private static final Logger log = Logger.getLogger(VisibilityFilter.class);
  
  public VisibilityFilter(SortedKeyValueIterator<Key,Value> iterator, Authorizations authorizations, byte[] defaultVisibility) {
    setSource(iterator);
    this.auths = authorizations;
    this.defaultVisibility = new Text(defaultVisibility);
    this.cache = new LRUMap(1000);
    this.tmpVis = new Text();
  }

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    if(source instanceof Filterer)
      ((Filterer<Key,Value>)source).applyFilter(new ColumnVisibilityPredicate(auths), false);
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    return new VisibilityFilter(getSource().deepCopy(env), auths, TextUtil.getBytes(defaultVisibility));
  }
  
  @Override
  public boolean accept(Key k, Value v) {
    Text testVis = k.getColumnVisibility(tmpVis);
    
    if (testVis.getLength() == 0 && defaultVisibility.getLength() == 0)
      return true;
    else if (testVis.getLength() == 0)
      testVis = defaultVisibility;
    
    Boolean b = (Boolean) cache.get(testVis);
    if (b != null)
      return b;
    
    Boolean bb = new ColumnVisibility(testVis).evaluate(auths);
    cache.put(new Text(testVis), bb);
    return bb;
  }
}
