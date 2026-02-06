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
package org.apache.accumulo.core.fate;

import java.util.UUID;

import org.apache.accumulo.core.fate.thrift.TFatePartition;

public record FatePartition(FateId start, FateId end) {

  public TFatePartition toThrift() {
    return new TFatePartition(start.canonical(), end.canonical());
  }

  public static FatePartition from(TFatePartition tfp) {
    return new FatePartition(FateId.from(tfp.start), FateId.from(tfp.stop));
  }

  private static final FatePartition ALL_USER =
      new FatePartition(FateId.from(FateInstanceType.USER, new UUID(0, 0)),
          FateId.from(FateInstanceType.USER, new UUID(-1, -1)));
  private static final FatePartition ALL_META =
      new FatePartition(FateId.from(FateInstanceType.META, new UUID(0, 0)),
          FateId.from(FateInstanceType.META, new UUID(-1, -1)));

  public static FatePartition all(FateInstanceType type) {
    return switch (type) {
      case META -> ALL_META;
      case USER -> ALL_USER;
    };
  }

  private static final UUID LAST_UUID = new UUID(-1, -1);

  public boolean isEndInclusive() {
    return end.getTxUUID().equals(LAST_UUID);
  }

  public boolean contains(FateId fateId) {
    if (isEndInclusive()) {
      return start.compareTo(fateId) >= 0 && end.compareTo(fateId) <= 0;
    } else {
      return start.compareTo(fateId) >= 0 && end.compareTo(fateId) < 0;
    }

  }
}
