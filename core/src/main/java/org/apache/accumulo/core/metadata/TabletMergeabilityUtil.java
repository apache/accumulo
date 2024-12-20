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
package org.apache.accumulo.core.metadata;

import java.time.Duration;

import org.apache.accumulo.core.client.admin.TabletMergeability;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.time.SteadyTime;

public class TabletMergeabilityUtil {

  public static Value toValue(TabletMergeability tabletMergeability) {
    return new Value(Long.toString(tabletMergeability.getDelay().toNanos()));
  }

  public static TabletMergeability fromValue(Value value) {
    return TabletMergeability.from(Duration.ofNanos(Long.parseLong(value.toString())));
  }

  public boolean isMergeable(TabletMergeability mergeability, SteadyTime currentTime) {
    if (mergeability.isNever()) {
      return false;
    }
    return currentTime.getDuration().compareTo(mergeability.getDelay()) >= 0;
  }

}
