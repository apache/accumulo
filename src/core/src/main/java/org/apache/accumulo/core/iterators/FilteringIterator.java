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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.filter.Filter;
import org.apache.accumulo.start.classloader.AccumuloClassLoader;
import org.apache.log4j.Logger;

/**
 * @deprecated since 1.4
 * @use org.apache.accumulo.core.iterators.Filter
 **/
public class FilteringIterator extends WrappingIterator implements OptionDescriber {
  private List<? extends Filter> filters;
  
  private static final Logger log = Logger.getLogger(IteratorUtil.class);
  
  public FilteringIterator deepCopy(IteratorEnvironment env) {
    return new FilteringIterator(this, env);
  }
  
  private FilteringIterator(FilteringIterator other, IteratorEnvironment env) {
    setSource(other.getSource().deepCopy(env));
    filters = other.filters;
  }
  
  public FilteringIterator() {}
  
  public FilteringIterator(SortedKeyValueIterator<Key,Value> iterator, List<? extends Filter> filters) throws IOException {
    this.setSource(iterator);
    this.filters = filters;
  }
  
  @Override
  public void next() throws IOException {
    super.next();
    findTop();
  }
  
  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    super.seek(range, columnFamilies, inclusive);
    findTop();
  }
  
  private void findTop() throws IOException {
    boolean goodKey;
    while (getSource().hasTop()) {
      goodKey = true;
      for (Filter f : filters) {
        if (!f.accept(getSource().getTopKey(), getSource().getTopValue())) {
          goodKey = false;
          break;
        }
      }
      if (goodKey == true)
        return;
      getSource().next();
    }
  }
  
  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    
    Map<String,Map<String,String>> classesToOptions = parseOptions(options);
    
    ArrayList<Filter> newFilters = new ArrayList<Filter>(classesToOptions.size());
    
    Collection<String> classes = classesToOptions.keySet();
    try {
      for (String filterClass : classes) {
        Class<? extends Filter> clazz = AccumuloClassLoader.loadClass(filterClass, Filter.class);
        Filter f = clazz.newInstance();
        f.init(classesToOptions.get(filterClass));
        newFilters.add(f);
      }
      
      filters = newFilters;
      
    } catch (ClassNotFoundException e) {
      log.error(e.toString());
      throw new IllegalArgumentException(e);
    } catch (InstantiationException e) {
      log.error(e.toString());
      throw new IllegalArgumentException(e);
    } catch (IllegalAccessException e) {
      log.error(e.toString());
      throw new IllegalArgumentException(e);
    }
  }
  
  /*
   * FilteringIterator expects its options in the following form:
   * 
   * key value 0 classNameA 0.optname1 optvalue1 0.optname2 optvalue2 1 classNameB 1.optname3 optvalue3
   * 
   * The initial digit is used only to distinguish the different filter classes. Additional options need not be provided, unless expected by the particular
   * filter.
   */
  
  private static Map<String,Map<String,String>> parseOptions(Map<String,String> options) {
    HashMap<String,String> namesToClasses = new HashMap<String,String>();
    HashMap<String,Map<String,String>> namesToOptions = new HashMap<String,Map<String,String>>();
    HashMap<String,Map<String,String>> classesToOptions = new HashMap<String,Map<String,String>>();
    
    int index;
    String name;
    String subName;
    
    Collection<Entry<String,String>> entries = options.entrySet();
    for (Entry<String,String> e : entries) {
      name = e.getKey();
      if ((index = name.indexOf(".")) < 0)
        namesToClasses.put(name, e.getValue());
      else {
        subName = name.substring(0, index);
        if (!namesToOptions.containsKey(subName))
          namesToOptions.put(subName, new HashMap<String,String>());
        namesToOptions.get(subName).put(name.substring(index + 1), e.getValue());
      }
    }
    
    Collection<String> names = namesToClasses.keySet();
    for (String s : names) {
      classesToOptions.put(namesToClasses.get(s), namesToOptions.get(s));
    }
    
    return classesToOptions;
  }
  
  @Override
  public IteratorOptions describeOptions() {
    return new IteratorOptions("filter", "FilteringIterator uses Filters to accept or reject key/value pairs", null,
        Collections.singletonList("<filterPriorityNumber> <ageoff|regex|filterClass>"));
  }
  
  @Override
  public boolean validateOptions(Map<String,String> options) {
    parseOptions(options);
    return true;
  }
}
