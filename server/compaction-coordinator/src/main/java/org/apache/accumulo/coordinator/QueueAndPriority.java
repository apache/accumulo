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
package org.apache.accumulo.coordinator;

import java.util.WeakHashMap;

import org.apache.accumulo.core.util.Pair;

public class QueueAndPriority implements Comparable<QueueAndPriority> {

  private static WeakHashMap<Pair<String,Short>,QueueAndPriority> CACHE = new WeakHashMap<>();

  public static QueueAndPriority get(String queue, short priority) {
    return CACHE.computeIfAbsent(new Pair<>(queue, priority),
        k -> new QueueAndPriority(queue, priority));
  }

  private final String queue;
  private final short priority;

  private QueueAndPriority(String queue, short priority) {
    this.queue = queue;
    this.priority = priority;
  }

  public String getQueue() {
    return queue;
  }

  public short getPriority() {
    return priority;
  }

  @Override
  public int hashCode() {
    return queue.hashCode() + ((Short) priority).hashCode();
  }

  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder();
    buf.append("queue: ").append(queue);
    buf.append(", priority: ").append(priority);
    return buf.toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (null == obj) {
      return false;
    }
    if (obj == this) {
      return true;
    }
    if (!(obj instanceof QueueAndPriority)) {
      return false;
    } else {
      QueueAndPriority other = (QueueAndPriority) obj;
      return this.queue.equals(other.queue) && this.priority == other.priority;
    }
  }

  @Override
  public int compareTo(QueueAndPriority other) {
    int result = this.queue.compareTo(other.queue);
    if (result == 0) {
      // reversing order such that if other priority is lower, then this has a higher return value
      return Long.compare(other.priority, this.priority);
    } else {
      return result;
    }
  }

}
