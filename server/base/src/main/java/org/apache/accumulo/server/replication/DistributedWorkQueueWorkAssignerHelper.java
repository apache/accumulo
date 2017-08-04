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
package org.apache.accumulo.server.replication;

import static java.util.Objects.requireNonNull;

import java.util.Map.Entry;

import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.replication.ReplicationTarget;
import org.apache.accumulo.server.zookeeper.DistributedWorkQueue;

import com.google.common.collect.Maps;

/**
 *
 */
public class DistributedWorkQueueWorkAssignerHelper {

  public static final String KEY_SEPARATOR = "|";

  /**
   * Serialize a filename and a {@link ReplicationTarget} into the expected key format for use with the {@link DistributedWorkQueue}
   *
   * @param filename
   *          Filename for data to be replicated
   * @param replTarget
   *          Information about replication peer
   * @return Key for identifying work in queue
   */
  public static String getQueueKey(String filename, ReplicationTarget replTarget) {
    return filename + KEY_SEPARATOR + replTarget.getPeerName() + KEY_SEPARATOR + replTarget.getRemoteIdentifier() + KEY_SEPARATOR
        + replTarget.getSourceTableId();
  }

  /**
   * @param queueKey
   *          Key from the work queue
   * @return Components which created the queue key
   */
  public static Entry<String,ReplicationTarget> fromQueueKey(String queueKey) {
    requireNonNull(queueKey);

    int index = queueKey.indexOf(KEY_SEPARATOR);
    if (-1 == index) {
      throw new IllegalArgumentException("Could not find expected separator in queue key '" + queueKey + "'");
    }

    String filename = queueKey.substring(0, index);

    int secondIndex = queueKey.indexOf(KEY_SEPARATOR, index + 1);
    if (-1 == secondIndex) {
      throw new IllegalArgumentException("Could not find expected separator in queue key '" + queueKey + "'");
    }

    int thirdIndex = queueKey.indexOf(KEY_SEPARATOR, secondIndex + 1);
    if (-1 == thirdIndex) {
      throw new IllegalArgumentException("Could not find expected seperator in queue key '" + queueKey + "'");
    }

    return Maps.immutableEntry(filename, new ReplicationTarget(queueKey.substring(index + 1, secondIndex), queueKey.substring(secondIndex + 1, thirdIndex),
        Table.ID.of(queueKey.substring(thirdIndex + 1))));
  }
}
