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
package org.apache.accumulo.core.client;

import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.util.ArgumentChecker;

/**
 * Configure a scan-time iterator.
 * 
 * Every iterator has a priority, a name, a class and any number of named configuration parameters. The typical use case:
 * 
 * <pre>
 * IteratorSetting cfg = new IteratorSetting(priority, &quot;myIter&quot;, MyIterator.class);
 * MyIterator.setOption(cfg, 42);
 * scanner.addScanIterator(cfg);
 * </pre>
 * 
 */
public class IteratorSetting {
  private int priority;
  private String name;
  private String iteratorClass;
  private Map<IteratorScope,Map<String,String>> properties = new EnumMap<IteratorScope,Map<String,String>>(IteratorScope.class);
  
  /**
   * Set layer at which this iterator applies. See {@link #setPriority(int) for how the priority is used.}
   * 
   * @return the priority of this Iterator
   */
  public int getPriority() {
    return priority;
  }
  
  /**
   * @param priority
   *          determines the order in which iterators are applied (system iterators are always applied first, then per-table and scan-time, lowest first)
   */
  public void setPriority(int priority) {
    this.priority = priority;
  }
  
  /**
   * Get the iterator's name.
   * 
   * @return the name of the iterator
   */
  public String getName() {
    return name;
  }
  
  /**
   * Set the iterator's name. Must be a simple alphanumeric identifier.
   * 
   * @param name
   */
  public void setName(String name) {
    this.name = name;
  }
  
  /**
   * Get the name of the class that implements the iterator.
   * 
   * @return the iterator's class name
   */
  public String getIteratorClass() {
    return iteratorClass;
  }
  
  /**
   * Set the name of the class that implements the iterator. The class does not have to be present on the client, but it must be available to all tablet
   * servers.
   * 
   * @param iteratorClass
   */
  public void setIteratorClass(String iteratorClass) {
    this.iteratorClass = iteratorClass;
  }
  
  /**
   * Get any properties set on the iterator. Note that this will be a merged view of the settings for all scopes.
   * 
   * @return a read-only copy of the named options (for all scopes)
   */
  public Map<String,String> getProperties() {
    Map<String,String> result = new HashMap<String,String>();
    for (Map<String,String> props : properties.values()) {
      result.putAll(props);
    }
    return result;
  }
  
  /**
   * Construct a basic iterator setting with all the required values.
   * 
   * @param priority
   *          the priority for the iterator @see {@link #setPriority(int)}
   * @param name
   *          the name for the iterator
   * @param iteratorClass
   *          the full class name for the iterator
   */
  public IteratorSetting(int priority, String name, String iteratorClass) {
    ArgumentChecker.notNull(name, iteratorClass);
    this.priority = priority;
    this.name = name;
    this.iteratorClass = iteratorClass;
  }
  
  /**
   * Construct a basic iterator setting.
   * 
   * @param priority
   *          the priority for the iterator @see {@link #setPriority(int)}
   * @param name
   *          the name for the iterator
   * @param iteratorClass
   *          the full class name for the iterator
   * @param scope
   *          the scope of the iterator
   * @param properties
   *          any properties for the iterator
   */
  public IteratorSetting(int priority, String name, String iteratorClass, IteratorScope scope, Map<String,String> properties) {
    ArgumentChecker.notNull(name, iteratorClass, scope);
    this.priority = priority;
    this.name = name;
    this.iteratorClass = iteratorClass;
    addOptions(scope, properties);
  }
  
  /**
   * @param priority
   *          the priority for the iterator @see {@link #setPriority(int)}
   * @param name
   *          the name for the iterator
   * @param iteratorClass
   *          the class to use for the class name
   */
  public IteratorSetting(int priority, String name, Class<? extends SortedKeyValueIterator<Key,Value>> iteratorClass) {
    this(priority, name, iteratorClass.getName());
  }
  
  /**
   * Constructs an ScanIterator using the given class's SimpleName for the iterator name
   * 
   * @param priority
   *          the priority for the iterator
   * @param iteratorClass
   *          the class to use for the iterator
   */
  public IteratorSetting(int priority, Class<? extends SortedKeyValueIterator<Key,Value>> iteratorClass) {
    this(priority, iteratorClass.getSimpleName(), iteratorClass.getName());
  }
  
  /**
   * Add another option to the iterator
   * 
   * @param option
   *          the name of the option
   * @param value
   *          the value of the option
   */
  public void addOption(String option, String value) {
    ArgumentChecker.notNull(option, value);
    for (IteratorScope scope : IteratorScope.values()) {
      addOptions(scope, Collections.singletonMap(option, value));
    }
  }
  
  /**
   * Add many options to the iterator
   * 
   * @param keyValues
   *          the values to add to the options
   */
  public void addOptions(Set<Entry<String,String>> keyValues) {
    for (Entry<String,String> keyValue : keyValues) {
      addOption(keyValue.getKey(), keyValue.getValue());
    }
  }
  
  /**
   * Get the properties on the iterator by scope
   * 
   * @return a mapping from scope to key/value settings
   */
  public Map<IteratorScope,Map<String,String>> getOptionsByScope() {
    Map<IteratorScope,Map<String,String>> result = new EnumMap<IteratorScope,Map<String,String>>(IteratorScope.class);
    for (Entry<IteratorScope,Map<String,String>> property : properties.entrySet()) {
      result.put(property.getKey(), Collections.unmodifiableMap(property.getValue()));
    }
    return Collections.unmodifiableMap(result);
  }
  
  /**
   * Add many options to to the iterator for a scope
   * 
   * @param scope
   *          the scope to put the properties
   * @param properties
   *          key/value pairs
   */
  public void addOptions(IteratorScope scope, Map<String,String> properties) {
    if (properties == null) return;
    if (!this.properties.containsKey(scope)) this.properties.put(scope, new HashMap<String,String>());
    for (Entry<String,String> entry : properties.entrySet()) {
      this.properties.get(scope).put(entry.getKey(), entry.getValue());
    }
  }
  
  /**
   * Delete the properties per-scope
   * 
   * @param scope
   *          the scope to delete
   */
  public void deleteOptions(IteratorScope scope) {
    this.properties.remove(scope);
  }
}
