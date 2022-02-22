/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.fate;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FateStatus {

  private final List<TransactionStatus> transactions;
  private final Map<String,List<String>> danglingHeldLocks;
  private final Map<String,List<String>> danglingWaitingLocks;

  /**
   * Convert FATE transactions IDs in keys of map to format that used in printing and logging FATE
   * transactions ids. This is done so that if the map is printed, the output can be used to search
   * Accumulo's logs.
   */
  private Map<String,List<String>> convert(Map<Long,List<String>> danglocks) {
    if (danglocks.isEmpty()) {
      return Collections.emptyMap();
    }

    Map<String,List<String>> ret = new HashMap<>();
    for (Map.Entry<Long,List<String>> entry : danglocks.entrySet()) {
      ret.put(String.format("%016x", entry.getKey()),
          Collections.unmodifiableList(entry.getValue()));
    }
    return Collections.unmodifiableMap(ret);
  }

  public FateStatus(List<TransactionStatus> transactions, Map<Long,List<String>> danglingHeldLocks,
      Map<Long,List<String>> danglingWaitingLocks) {
    this.transactions = Collections.unmodifiableList(transactions);
    this.danglingHeldLocks = convert(danglingHeldLocks);
    this.danglingWaitingLocks = convert(danglingWaitingLocks);
  }

  public List<TransactionStatus> getTransactions() {
    return transactions;
  }

  /**
   * Get locks that are held by non existent FATE transactions. These are table or namespace locks.
   *
   * @return map where keys are transaction ids and values are a list of table IDs and/or namespace
   *         IDs. The transaction IDs are in the same format as transaction IDs in the Accumulo
   *         logs.
   */
  public Map<String,List<String>> getDanglingHeldLocks() {
    return danglingHeldLocks;
  }

  /**
   * Get locks that are waiting to be acquired by non existent FATE transactions. These are table or
   * namespace locks.
   *
   * @return map where keys are transaction ids and values are a list of table IDs and/or namespace
   *         IDs. The transaction IDs are in the same format as transaction IDs in the Accumulo
   *         logs.
   */
  public Map<String,List<String>> getDanglingWaitingLocks() {
    return danglingWaitingLocks;
  }
}
