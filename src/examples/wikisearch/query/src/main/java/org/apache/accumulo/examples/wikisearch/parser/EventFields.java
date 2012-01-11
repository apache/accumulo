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
package org.apache.accumulo.examples.wikisearch.parser;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.examples.wikisearch.parser.EventFields.FieldValue;

import com.esotericsoftware.kryo.CustomSerialization;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serialize.ArraySerializer;
import com.esotericsoftware.kryo.serialize.IntSerializer;
import com.esotericsoftware.kryo.serialize.StringSerializer;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;
import com.google.common.collect.SetMultimap;

/**
 * Object used to hold the fields in an event. This is a multimap because fields can be repeated.
 */
public class EventFields implements SetMultimap<String,FieldValue>, CustomSerialization {
  
  private static boolean kryoInitialized = false;
  private static ArraySerializer valueSerializer = null;
  
  private Multimap<String,FieldValue> map = null;
  
  public static class FieldValue {
    ColumnVisibility visibility;
    byte[] value;
    
    public FieldValue(ColumnVisibility visibility, byte[] value) {
      super();
      this.visibility = visibility;
      this.value = value;
    }
    
    public ColumnVisibility getVisibility() {
      return visibility;
    }
    
    public byte[] getValue() {
      return value;
    }
    
    public void setVisibility(ColumnVisibility visibility) {
      this.visibility = visibility;
    }
    
    public void setValue(byte[] value) {
      this.value = value;
    }
    
    public int size() {
      return visibility.flatten().length + value.length;
    }
    
    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder();
      if (null != visibility)
        buf.append(" visibility: ").append(new String(visibility.flatten()));
      if (null != value)
        buf.append(" value size: ").append(value.length);
      if (null != value)
        buf.append(" value: ").append(new String(value));
      return buf.toString();
    }
    
  }
  
  public EventFields() {
    map = HashMultimap.create();
  }
  
  public int size() {
    return map.size();
  }
  
  public boolean isEmpty() {
    return map.isEmpty();
  }
  
  public boolean containsKey(Object key) {
    return map.containsKey(key);
  }
  
  public boolean containsValue(Object value) {
    return map.containsValue(value);
  }
  
  public boolean containsEntry(Object key, Object value) {
    return map.containsEntry(key, value);
  }
  
  public boolean put(String key, FieldValue value) {
    return map.put(key, value);
  }
  
  public boolean remove(Object key, Object value) {
    return map.remove(key, value);
  }
  
  public boolean putAll(String key, Iterable<? extends FieldValue> values) {
    return map.putAll(key, values);
  }
  
  public boolean putAll(Multimap<? extends String,? extends FieldValue> multimap) {
    return map.putAll(multimap);
  }
  
  public void clear() {
    map.clear();
  }
  
  public Set<String> keySet() {
    return map.keySet();
  }
  
  public Multiset<String> keys() {
    return map.keys();
  }
  
  public Collection<FieldValue> values() {
    return map.values();
  }
  
  public Set<FieldValue> get(String key) {
    return (Set<FieldValue>) map.get(key);
  }
  
  public Set<FieldValue> removeAll(Object key) {
    return (Set<FieldValue>) map.removeAll(key);
  }
  
  public Set<FieldValue> replaceValues(String key, Iterable<? extends FieldValue> values) {
    return (Set<FieldValue>) map.replaceValues(key, values);
  }
  
  public Set<Entry<String,FieldValue>> entries() {
    return (Set<Entry<String,FieldValue>>) map.entries();
  }
  
  public Map<String,Collection<FieldValue>> asMap() {
    return map.asMap();
  }
  
  public int getByteSize() {
    int count = 0;
    for (Entry<String,FieldValue> e : map.entries()) {
      count += e.getKey().getBytes().length + e.getValue().size();
    }
    return count;
  }
  
  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder();
    for (Entry<String,FieldValue> entry : map.entries()) {
      buf.append("\tkey: ").append(entry.getKey()).append(" -> ").append(entry.getValue().toString()).append("\n");
    }
    return buf.toString();
  }
  
  public static synchronized void initializeKryo(Kryo kryo) {
    if (kryoInitialized)
      return;
    valueSerializer = new ArraySerializer(kryo);
    valueSerializer.setDimensionCount(1);
    valueSerializer.setElementsAreSameType(true);
    valueSerializer.setCanBeNull(false);
    valueSerializer.setElementsCanBeNull(false);
    kryo.register(byte[].class, valueSerializer);
    kryoInitialized = true;
  }
  
  public void readObjectData(Kryo kryo, ByteBuffer buf) {
    if (!kryoInitialized)
      EventFields.initializeKryo(kryo);
    // Read in the number of map entries
    int entries = IntSerializer.get(buf, true);
    for (int i = 0; i < entries; i++) {
      // Read in the key
      String key = StringSerializer.get(buf);
      // Read in the fields in the value
      ColumnVisibility vis = new ColumnVisibility(valueSerializer.readObjectData(buf, byte[].class));
      byte[] value = valueSerializer.readObjectData(buf, byte[].class);
      map.put(key, new FieldValue(vis, value));
    }
    
  }
  
  public void writeObjectData(Kryo kryo, ByteBuffer buf) {
    if (!kryoInitialized)
      EventFields.initializeKryo(kryo);
    // Write out the number of entries;
    IntSerializer.put(buf, map.size(), true);
    for (Entry<String,FieldValue> entry : map.entries()) {
      // Write the key
      StringSerializer.put(buf, entry.getKey());
      // Write the fields in the value
      valueSerializer.writeObjectData(buf, entry.getValue().getVisibility().flatten());
      valueSerializer.writeObjectData(buf, entry.getValue().getValue());
    }
  }
  
}
