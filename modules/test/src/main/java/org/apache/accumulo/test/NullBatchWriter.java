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
package org.apache.accumulo.test;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.data.Mutation;

public class NullBatchWriter implements BatchWriter {

  private int mutationsAdded;
  private long startTime;

  @Override
  public void addMutation(Mutation m) throws MutationsRejectedException {
    if (mutationsAdded == 0) {
      startTime = System.currentTimeMillis();
    }
    mutationsAdded++;
    m.numBytes();
  }

  @Override
  public void addMutations(Iterable<Mutation> iterable) throws MutationsRejectedException {
    for (Mutation mutation : iterable) {
      addMutation(mutation);
    }
  }

  @Override
  public void close() throws MutationsRejectedException {
    flush();
  }

  @Override
  public void flush() throws MutationsRejectedException {
    System.out.printf("Mutation add rate : %,6.2f mutations/sec%n", mutationsAdded / ((System.currentTimeMillis() - startTime) / 1000.0));
    mutationsAdded = 0;
  }

}
