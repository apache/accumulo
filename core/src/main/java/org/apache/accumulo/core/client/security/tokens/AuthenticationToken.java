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
package org.apache.accumulo.core.client.security.tokens;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.security.auth.DestroyFailedException;
import javax.security.auth.Destroyable;

import org.apache.hadoop.io.Writable;

/**
 * @since 1.5.0
 */
public interface AuthenticationToken extends Writable, Destroyable, Cloneable {
  public class Properties implements Destroyable, Map<String,char[]> {
    
    private boolean destroyed = false;
    private HashMap<String,char[]> map = new HashMap<String,char[]>();
    
    private void checkDestroyed() {
      if (destroyed)
        throw new IllegalStateException();
    }
    
    public char[] put(String key, CharSequence value) {
      checkDestroyed();
      char[] toPut = new char[value.length()];
      for (int i = 0; i < value.length(); i++)
        toPut[i] = value.charAt(i);
      return map.put(key, toPut);
    }
    
    public void putAllStrings(Map<String, ? extends CharSequence> map) {
      checkDestroyed();
      for (Map.Entry<String,? extends CharSequence> entry : map.entrySet()) {
        put(entry.getKey(), entry.getValue());
      }
    }

    @Override
    public void destroy() throws DestroyFailedException {
      for (String key : this.keySet()) {
        char[] val = this.get(key);
        Arrays.fill(val, (char) 0);
      }
      this.clear();
      destroyed = true;
    }

    @Override
    public boolean isDestroyed() {
      return destroyed;
    }
    
    @Override
    public int size() {
      checkDestroyed();
      return map.size();
    }
    
    @Override
    public boolean isEmpty() {
      checkDestroyed();
      return map.isEmpty();
    }
    
    @Override
    public boolean containsKey(Object key) {
      checkDestroyed();
      return map.containsKey(key);
    }
    
    @Override
    public boolean containsValue(Object value) {
      checkDestroyed();
      return map.containsValue(value);
    }
    
    @Override
    public char[] get(Object key) {
      checkDestroyed();
      return map.get(key);
    }
    
    @Override
    public char[] put(String key, char[] value) {
      checkDestroyed();
      return map.put(key, value);
    }
    
    @Override
    public char[] remove(Object key) {
      checkDestroyed();
      return map.remove(key);
    }
    
    @Override
    public void putAll(Map<? extends String,? extends char[]> m) {
      checkDestroyed();
      map.putAll(m);
    }
    
    @Override
    public void clear() {
      checkDestroyed();
      map.clear();
    }
    
    @Override
    public Set<String> keySet() {
      checkDestroyed();
      return map.keySet();
    }
    
    @Override
    public Collection<char[]> values() {
      checkDestroyed();
      return map.values();
    }
    
    @Override
    public Set<java.util.Map.Entry<String,char[]>> entrySet() {
      checkDestroyed();
      return map.entrySet();
    }
  }
  
  public static class TokenProperty implements Comparable<TokenProperty> {
    private String key, description;
    private boolean masked;
    
    public TokenProperty(String name, String description, boolean mask) {
      this.key = name;
      this.description = description;
      this.masked = mask;
    }
    
    public String toString() {
      return this.key + " - " + description;
    }
    
    public String getKey() {
      return this.key;
    }
    
    public String getDescription() {
      return this.description;
    }
    
    public boolean getMask() {
      return this.masked;
    }
    
    public int hashCode() {
      return key.hashCode();
    }
    
    public boolean equals(Object o) {
      if (o instanceof TokenProperty)
        return ((TokenProperty) o).key.equals(key);
      return false;
    }
    
    @Override
    public int compareTo(TokenProperty o) {
      return key.compareTo(o.key);
    }
  }
  
  public void init(Properties properties);
  
  public Set<TokenProperty> getProperties();
  public AuthenticationToken clone();
}
