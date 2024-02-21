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
package org.apache.accumulo.core.fate.accumulo;

import org.apache.accumulo.core.fate.Fate;
import org.apache.accumulo.core.fate.FateKey;
import org.apache.accumulo.core.fate.ReadOnlyFateStore.TStatus;
import org.apache.accumulo.core.fate.Repo;

public interface FateMutator<T> {

  FateMutator<T> putStatus(TStatus status);

  FateMutator<T> putKey(FateKey fateKey);

  FateMutator<T> putCreateTime(long ctime);

  FateMutator<T> putName(byte[] data);

  FateMutator<T> putAutoClean(byte[] data);

  FateMutator<T> putException(byte[] data);

  FateMutator<T> putReturnValue(byte[] data);

  FateMutator<T> putAgeOff(byte[] data);

  FateMutator<T> putTxInfo(Fate.TxInfo txInfo, byte[] data);

  FateMutator<T> putRepo(int position, Repo<T> repo);

  FateMutator<T> deleteRepo(int position);

  void mutate();

  // This exists to represent the subset of statuses from ConditionalWriter.Status that are expected
  // and need to be handled.
  enum Status {
    ACCEPTED, REJECTED, UNKNOWN
  }

  Status tryMutate();

}
