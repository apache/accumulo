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

import static com.google.common.base.Preconditions.checkArgument;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * Configure an iterator for minc, majc, and/or scan. By default, IteratorSetting will be configured for scan.
 *
 * Every iterator has a priority, a name, a class, a set of scopes, and configuration parameters.
 *
 * A typical use case configured for scan:
 *
 * <pre>
 * IteratorSetting cfg = new IteratorSetting(priority, &quot;myIter&quot;, MyIterator.class);
 * MyIterator.addOption(cfg, 42);
 * scanner.addScanIterator(cfg);
 * </pre>
 */
public class IteratorSetting implements Writable {
  private int priority;
  private String name;
  private String iteratorClass;
  private Map<String,String> properties;

  /**
   * Get layer at which this iterator applies. See {@link #setPriority(int)} for how the priority is used.
   *
   * @return the priority of this Iterator
   */
  public int getPriority() {
    return priority;
  }

  /**
   * Set layer at which this iterator applies.
   *
   * @param priority
   *          determines the order in which iterators are applied (system iterators are always applied first, then user-configured iterators, lowest priority
   *          first)
   */
  public void setPriority(int priority) {
    checkArgument(priority > 0, "property must be strictly positive");
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
   */
  public void setName(String name) {
    checkArgument(name != null, "name is null");
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
   */
  public void setIteratorClass(String iteratorClass) {
    checkArgument(iteratorClass != null, "iteratorClass is null");
    this.iteratorClass = iteratorClass;
  }

  /**
   * Constructs an iterator setting configured for the scan scope with no parameters. (Parameters can be added later.)
   *
   * @param priority
   *          the priority for the iterator (see {@link #setPriority(int)})
   * @param name
   *          the distinguishing name for the iterator
   * @param iteratorClass
   *          the fully qualified class name for the iterator
   */
  public IteratorSetting(int priority, String name, String iteratorClass) {
    this(priority, name, iteratorClass, new HashMap<String,String>());
  }

  /**
   * Constructs an iterator setting configured for the specified scopes with the specified parameters.
   *
   * @param priority
   *          the priority for the iterator (see {@link #setPriority(int)})
   * @param name
   *          the distinguishing name for the iterator
   * @param iteratorClass
   *          the fully qualified class name for the iterator
   * @param properties
   *          any properties for the iterator
   */
  public IteratorSetting(int priority, String name, String iteratorClass, Map<String,String> properties) {
    setPriority(priority);
    setName(name);
    setIteratorClass(iteratorClass);
    this.properties = new HashMap<>();
    addOptions(properties);
  }

  /**
   * Constructs an iterator setting using the given class's SimpleName for the iterator name. The iterator setting will be configured for the scan scope with no
   * parameters.
   *
   * @param priority
   *          the priority for the iterator (see {@link #setPriority(int)})
   * @param iteratorClass
   *          the class for the iterator
   */
  public IteratorSetting(int priority, Class<? extends SortedKeyValueIterator<Key,Value>> iteratorClass) {
    this(priority, iteratorClass.getSimpleName(), iteratorClass.getName());
  }

  /**
   *
   * Constructs an iterator setting using the given class's SimpleName for the iterator name and configured for the specified scopes with the specified
   * parameters.
   *
   * @param priority
   *          the priority for the iterator (see {@link #setPriority(int)})
   * @param iteratorClass
   *          the class for the iterator
   * @param properties
   *          any properties for the iterator
   */
  public IteratorSetting(int priority, Class<? extends SortedKeyValueIterator<Key,Value>> iteratorClass, Map<String,String> properties) {
    this(priority, iteratorClass.getSimpleName(), iteratorClass.getName(), properties);
  }

  /**
   * Constructs an iterator setting configured for the scan scope with no parameters.
   *
   * @param priority
   *          the priority for the iterator (see {@link #setPriority(int)})
   * @param name
   *          the distinguishing name for the iterator
   * @param iteratorClass
   *          the class for the iterator
   */
  public IteratorSetting(int priority, String name, Class<? extends SortedKeyValueIterator<Key,Value>> iteratorClass) {
    this(priority, name, iteratorClass.getName());
  }

  /**
   * Constructs an iterator setting using the provided name and the provided class's name for the scan scope with the provided parameters.
   *
   * @param priority
   *          The priority for the iterator (see {@link #setPriority(int)})
   * @param name
   *          The distinguishing name for the iterator
   * @param iteratorClass
   *          The class for the iterator
   * @param properties
   *          Any properties for the iterator
   *
   * @since 1.6.0
   */
  public IteratorSetting(int priority, String name, Class<? extends SortedKeyValueIterator<Key,Value>> iteratorClass, Map<String,String> properties) {
    this(priority, name, iteratorClass.getName(), properties);
  }

  /**
   * @since 1.5.0
   */
  public IteratorSetting(DataInput din) throws IOException {
    this.properties = new HashMap<>();
    this.readFields(din);
  }

  /**
   * Add another option to the iterator.
   *
   * @param option
   *          the name of the option
   * @param value
   *          the value of the option
   */
  public void addOption(String option, String value) {
    checkArgument(option != null, "option is null");
    checkArgument(value != null, "value is null");
    properties.put(option, value);
  }

  /**
   * Remove an option from the iterator.
   *
   * @param option
   *          the name of the option
   * @return the value previously associated with the option, or null if no such option existed
   */
  public String removeOption(String option) {
    checkArgument(option != null, "option is null");
    return properties.remove(option);
  }

  /**
   * Add many options to the iterator.
   *
   * @param propertyEntries
   *          a set of entries to add to the options
   */
  public void addOptions(Set<Entry<String,String>> propertyEntries) {
    checkArgument(propertyEntries != null, "propertyEntries is null");
    for (Entry<String,String> keyValue : propertyEntries) {
      addOption(keyValue.getKey(), keyValue.getValue());
    }
  }

  /**
   * Add many options to the iterator.
   *
   * @param properties
   *          a map of entries to add to the options
   */
  public void addOptions(Map<String,String> properties) {
    checkArgument(properties != null, "properties is null");
    addOptions(properties.entrySet());
  }

  /**
   * Get the configuration parameters for this iterator.
   *
   * @return the properties
   */
  public Map<String,String> getOptions() {
    return Collections.unmodifiableMap(properties);
  }

  /**
   * Remove all options from the iterator.
   */
  public void clearOptions() {
    properties.clear();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((iteratorClass == null) ? 0 : iteratorClass.hashCode());
    result = prime * result + ((name == null) ? 0 : name.hashCode());
    result = prime * result + priority;
    result = prime * result + ((properties == null) ? 0 : properties.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (!(obj instanceof IteratorSetting))
      return false;
    IteratorSetting other = (IteratorSetting) obj;
    if (iteratorClass == null) {
      if (other.iteratorClass != null)
        return false;
    } else if (!iteratorClass.equals(other.iteratorClass))
      return false;
    if (name == null) {
      if (other.name != null)
        return false;
    } else if (!name.equals(other.name))
      return false;
    if (priority != other.priority)
      return false;
    if (properties == null) {
      if (other.properties != null)
        return false;
    } else if (!properties.equals(other.properties))
      return false;
    return true;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("name:");
    sb.append(name);
    sb.append(", priority:");
    sb.append(Integer.toString(priority));
    sb.append(", class:");
    sb.append(iteratorClass);
    sb.append(", properties:");
    sb.append(properties);
    return sb.toString();
  }

  /**
   * A convenience class for passing column family and column qualifiers to iterator configuration methods.
   */
  public static class Column extends Pair<Text,Text> {

    public Column(Text columnFamily, Text columnQualifier) {
      super(columnFamily, columnQualifier);
    }

    public Column(Text columnFamily) {
      super(columnFamily, null);
    }

    public Column(String columnFamily, String columnQualifier) {
      super(new Text(columnFamily), new Text(columnQualifier));
    }

    public Column(String columnFamily) {
      super(new Text(columnFamily), null);
    }

    public Text getColumnFamily() {
      return getFirst();
    }

    public Text getColumnQualifier() {
      return getSecond();
    }

  }

  /**
   * @since 1.5.0
   * @see Writable
   */
  @Override
  public void readFields(DataInput din) throws IOException {
    priority = WritableUtils.readVInt(din);
    name = WritableUtils.readString(din);
    iteratorClass = WritableUtils.readString(din);
    properties.clear();
    int size = WritableUtils.readVInt(din);
    while (size > 0) {
      properties.put(WritableUtils.readString(din), WritableUtils.readString(din));
      size--;
    }
  }

  /**
   * @since 1.5.0
   * @see Writable
   */
  @Override
  public void write(DataOutput dout) throws IOException {
    WritableUtils.writeVInt(dout, priority);
    WritableUtils.writeString(dout, name);
    WritableUtils.writeString(dout, iteratorClass);
    WritableUtils.writeVInt(dout, properties.size());
    for (Entry<String,String> e : properties.entrySet()) {
      WritableUtils.writeString(dout, e.getKey());
      WritableUtils.writeString(dout, e.getValue());
    }
  }
}
