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
package org.apache.accumulo.core.fate.user;

import static org.apache.accumulo.core.fate.user.schema.FateSchema.TxColumnFamily.RESERVATION_COLUMN;

import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Condition;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.fate.FateStore;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

/**
 * An iterator used for determining if the reservation column for a FateId has a FateReservation set
 * or not. Maps the value of the column to "isreserved" or "notreserved" if the column has a
 * FateReservation value set or not.
 */
public class ReservationMappingIterator extends ColumnValueMappingIterator {

  private static final String IS_RESERVED = "isreserved";
  private static final String NOT_RESERVED = "notreserved";

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) throws IOException {
    this.source = source;
    // No need for options or env
  }

  @Override
  protected void mapValue() {
    if (hasTop()) {
      String currVal = source.getTopValue().toString();
      mappedValue = FateStore.FateReservation.isFateReservation(currVal) ? new Value(IS_RESERVED)
          : new Value(NOT_RESERVED);
    } else {
      mappedValue = null;
    }
  }

  public static Condition createRequireReservedCondition() {
    Condition condition = new Condition(RESERVATION_COLUMN.getColumnFamily(),
        RESERVATION_COLUMN.getColumnQualifier());
    IteratorSetting is = new IteratorSetting(100, ReservationMappingIterator.class);

    return condition.setValue(IS_RESERVED).setIterators(is);
  }

  public static Condition createRequireUnreservedCondition() {
    Condition condition = new Condition(RESERVATION_COLUMN.getColumnFamily(),
        RESERVATION_COLUMN.getColumnQualifier());
    IteratorSetting is = new IteratorSetting(100, ReservationMappingIterator.class);

    return condition.setValue(NOT_RESERVED).setIterators(is);
  }
}
