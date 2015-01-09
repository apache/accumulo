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

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.Credentials;

public class TabletServerBatchDeleter extends TabletServerBatchReader implements BatchDeleter {

  private Instance instance;
  private Credentials credentials;
  private String tableId;
  private BatchWriterConfig bwConfig;

  public TabletServerBatchDeleter(Instance instance, Credentials credentials, String tableId, Authorizations authorizations, int numQueryThreads,
      BatchWriterConfig bwConfig) throws TableNotFoundException {
    super(instance, credentials, tableId, authorizations, numQueryThreads);
    this.instance = instance;
    this.credentials = credentials;
    this.tableId = tableId;
    this.bwConfig = bwConfig;
    super.addScanIterator(new IteratorSetting(Integer.MAX_VALUE, BatchDeleter.class.getName() + ".NOVALUE", SortedKeyIterator.class));
  }

  @Override
  public void delete() throws MutationsRejectedException, TableNotFoundException {
    BatchWriter bw = null;
    try {
      bw = new BatchWriterImpl(instance, credentials, tableId, bwConfig);
      Iterator<Entry<Key,Value>> iter = super.iterator();
      while (iter.hasNext()) {
        Entry<Key,Value> next = iter.next();
        Key k = next.getKey();
        Mutation m = new Mutation(k.getRow());
        m.putDelete(k.getColumnFamily(), k.getColumnQualifier(), new ColumnVisibility(k.getColumnVisibility()), k.getTimestamp());
        bw.addMutation(m);
      }
    } finally {
      if (bw != null)
        bw.close();
    }
  }

}
