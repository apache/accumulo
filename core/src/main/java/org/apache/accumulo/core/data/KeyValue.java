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
package org.apache.accumulo.core.data;

import java.nio.ByteBuffer;
import java.util.AbstractMap.SimpleImmutableEntry;

/**
 * A key/value pair. The key and value may not be set after construction.
 */
public class KeyValue extends SimpleImmutableEntry<Key,Value> {

  private static final long serialVersionUID = 1L;

  /**
   * Creates a new key/value pair.
   *
   * @param key key
   * @param value bytes of value
   */
  public KeyValue(Key key, byte[] value) {
    super(key, new Value(value, false));
  }

  /**
   * Creates a new key/value pair.
   *
   * @param key key
   * @param value buffer containing bytes of value
   */
  public KeyValue(Key key, ByteBuffer value) {
    super(key, new Value(value));
  }

  /**
   * Creates a new key/value pair.
   *
   * @param key key
   * @param value buffer containing bytes of value
   */
  public KeyValue(Key key, Value value) {
    super(key, value);
  }
}
