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

import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;

public class MockMultiTableBatchWriter implements MultiTableBatchWriter {
  MockAccumulo acu = null;
  Map<String,MockBatchWriter> bws = null;

  public MockMultiTableBatchWriter(MockAccumulo acu) {
    this.acu = acu;
    bws = new HashMap<String,MockBatchWriter>();
  }

  @Override
  public BatchWriter getBatchWriter(String table) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    if (!bws.containsKey(table)) {
      bws.put(table, new MockBatchWriter(acu, table));
    }
    return bws.get(table);
  }

  @Override
  public void flush() throws MutationsRejectedException {}

  @Override
  public void close() throws MutationsRejectedException {}

  @Override
  public boolean isClosed() {
    throw new UnsupportedOperationException();
  }
}
