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
package org.apache.accumulo.core.data;

import static org.apache.accumulo.core.util.ByteBufferUtil.toBytes;

import java.nio.ByteBuffer;
import java.util.Map;

public class KeyValue implements Map.Entry<Key,Value> {
  
  public Key key;
  public byte[] value;
  
  public KeyValue(Key key, byte[] value) {
    this.key = key;
    this.value = value;
  }
  
  public KeyValue(Key key, ByteBuffer value) {
    this.key = key;
    this.value = toBytes(value);
  }
  
  @Override
  public Key getKey() {
    return key;
  }
  
  @Override
  public Value getValue() {
    return new Value(value);
  }
  
  @Override
  public Value setValue(Value value) {
    throw new UnsupportedOperationException();
  }
  
  public String toString() {
    return key + " " + new String(value);
  }
  
}
