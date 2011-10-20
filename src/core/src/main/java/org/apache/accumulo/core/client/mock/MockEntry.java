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
package org.apache.accumulo.core.client.mock;

import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

class MockEntry implements Entry<Key,Value> {
  Key key;
  Value value;
  
  public MockEntry(Key key, Value value) {
    this.key = key;
    this.value = value;
  }
  
  @Override
  public Key getKey() {
    return key;
  }
  
  @Override
  public Value getValue() {
    return value;
  }
  
  @Override
  public Value setValue(Value value) {
    Value result = this.value;
    this.value = value;
    return result;
  }
  
  @Override
  public String toString() {
    return key.toString() + " -> " + value.toString();
  }
  
  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof MockEntry)) {
      return false;
    }
    MockEntry entry = (MockEntry) obj;
    return key.equals(entry.key) && value.equals(entry.value);
  }
  
}