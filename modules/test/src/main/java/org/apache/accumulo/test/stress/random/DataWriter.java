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
package org.apache.accumulo.test.stress.random;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;

public class DataWriter extends Stream<Void> {
  private final BatchWriter writer;
  private final RandomMutations mutations;

  public DataWriter(BatchWriter writer, RandomMutations mutations) {
    this.writer = writer;
    this.mutations = mutations;
  }

  @Override
  public Void next() {
    try {
      writer.addMutation(mutations.next());
    } catch (MutationsRejectedException e) {
      throw new RuntimeException(e);
    }
    return null;
  }

  @Override
  public void finalize() {
    try {
      this.writer.close();
    } catch (MutationsRejectedException e) {
      System.err.println("Error closing batch writer.");
      e.printStackTrace();
    }
  }
}
