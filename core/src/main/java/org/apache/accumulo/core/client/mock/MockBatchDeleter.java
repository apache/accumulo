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

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;

/**
 * {@link BatchDeleter} for a {@link MockAccumulo} instance. Behaves similarly to a regular {@link BatchDeleter}, with a few exceptions:
 * <ol>
 * <li>There is no waiting for memory to fill before flushing</li>
 * <li>Only one thread is used for writing</li>
 * </ol>
 *
 * Otherwise, it behaves as expected.
 */
public class MockBatchDeleter extends MockBatchScanner implements BatchDeleter {

  private final MockAccumulo acc;
  private final String tableName;

  /**
   * Create a {@link BatchDeleter} for the specified instance on the specified table where the writer uses the specified {@link Authorizations}.
   */
  public MockBatchDeleter(MockAccumulo acc, String tableName, Authorizations auths) {
    super(acc.tables.get(tableName), auths);
    this.acc = acc;
    this.tableName = tableName;
  }

  @Override
  public void delete() throws MutationsRejectedException, TableNotFoundException {

    BatchWriter writer = new MockBatchWriter(acc, tableName);
    try {
      Iterator<Entry<Key,Value>> iter = super.iterator();
      while (iter.hasNext()) {
        Entry<Key,Value> next = iter.next();
        Key k = next.getKey();
        Mutation m = new Mutation(k.getRow());
        m.putDelete(k.getColumnFamily(), k.getColumnQualifier(), new ColumnVisibility(k.getColumnVisibility()), k.getTimestamp());
        writer.addMutation(m);
      }
    } finally {
      writer.close();
    }
  }

}
