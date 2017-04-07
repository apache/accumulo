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
package org.apache.accumulo.core.client.mock;

import static com.google.common.base.Preconditions.checkArgument;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.data.Mutation;

/**
 * @deprecated since 1.8.0; use MiniAccumuloCluster or a standard mock framework instead.
 */
@Deprecated
public class MockBatchWriter implements BatchWriter {

  final String tablename;
  final MockAccumulo acu;

  MockBatchWriter(MockAccumulo acu, String tablename) {
    this.acu = acu;
    this.tablename = tablename;
  }

  @Override
  public void addMutation(Mutation m) throws MutationsRejectedException {
    checkArgument(m != null, "m is null");
    acu.addMutation(tablename, m);
  }

  @Override
  public void addMutations(Iterable<Mutation> iterable) throws MutationsRejectedException {
    checkArgument(iterable != null, "iterable is null");
    for (Mutation m : iterable) {
      acu.addMutation(tablename, m);
    }
  }

  @Override
  public void flush() throws MutationsRejectedException {}

  @Override
  public void close() throws MutationsRejectedException {}

}
