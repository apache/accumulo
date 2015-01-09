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
package org.apache.accumulo.core.client.impl;

import org.apache.accumulo.core.client.Durability;
import org.apache.accumulo.core.tabletserver.thrift.TDurability;

public class DurabilityImpl {

  public static TDurability toThrift(Durability durability) {
    switch (durability) {
      case DEFAULT:
        return TDurability.DEFAULT;
      case SYNC:
        return TDurability.SYNC;
      case FLUSH:
        return TDurability.FLUSH;
      case LOG:
        return TDurability.LOG;
      default:
        return TDurability.NONE;
    }
  }

  public static Durability fromString(String value) {
    return Durability.valueOf(value.toUpperCase());
  }

  public static Durability fromThrift(TDurability tdurabilty) {
    if (tdurabilty == null) {
      return Durability.DEFAULT;
    }
    switch (tdurabilty) {
      case DEFAULT:
        return Durability.DEFAULT;
      case SYNC:
        return Durability.SYNC;
      case FLUSH:
        return Durability.FLUSH;
      case LOG:
        return Durability.LOG;
      default:
        return Durability.NONE;
    }
  }

  public static Durability resolveDurabilty(Durability durability, Durability tabletDurability) {
    if (durability == Durability.DEFAULT) {
      return tabletDurability;
    }
    return durability;
  }

}
