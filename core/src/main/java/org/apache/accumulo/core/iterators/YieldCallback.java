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

/**
 * This callback handles the state of yielding within an iterator
 */
public class YieldCallback<K> {
  private K key;

  /**
   * Called by the iterator when a next or seek call yields control.
   *
   * @param key
   *          the key position at which the iterator yielded.
   */
  public void yield(K key) {
    this.key = key;
  }

  /**
   * Called by the client to see if the iterator yielded
   *
   * @return true if iterator yielded control
   */
  public boolean hasYielded() {
    return (this.key != null);
  }

  /**
   * Called by the client to get the yield position used as the start key (non-inclusive) of the range in a subsequent seek call when the iterator is rebuilt.
   * This will also reset the state returned by hasYielded.
   *
   * @return <tt>K</tt> The key position
   */
  public K getPositionAndReset() {
    try {
      return this.key;
    } finally {
      this.key = null;
    }
  }
}
