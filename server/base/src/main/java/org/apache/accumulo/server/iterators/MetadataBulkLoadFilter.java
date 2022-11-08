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
package org.apache.accumulo.server.iterators;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.BulkFileColumnFamily;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.zookeeper.TransactionWatcher.Arbitrator;
import org.apache.accumulo.server.zookeeper.TransactionWatcher.ZooArbitrator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A special iterator for the metadata table that removes inactive bulk load flags
 */
public class MetadataBulkLoadFilter extends Filter {
  private static final Logger log = LoggerFactory.getLogger(MetadataBulkLoadFilter.class);

  enum Status {
    ACTIVE, INACTIVE
  }

  Map<Long,Status> bulkTxStatusCache;
  Arbitrator arbitrator;

  @Override
  public boolean accept(Key k, Value v) {
    if (!k.isDeleted() && k.compareColumnFamily(BulkFileColumnFamily.NAME) == 0) {
      long txid = BulkFileColumnFamily.getBulkLoadTid(v);

      Status status = bulkTxStatusCache.get(txid);
      if (status == null) {
        try {
          if (arbitrator.transactionComplete(Constants.BULK_ARBITRATOR_TYPE, txid)) {
            status = Status.INACTIVE;
          } else {
            status = Status.ACTIVE;
          }
        } catch (Exception e) {
          status = Status.ACTIVE;
          log.error("{}", e.getMessage(), e);
        }

        bulkTxStatusCache.put(txid, status);
      }

      return status == Status.ACTIVE;
    }

    return true;
  }

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) throws IOException {
    super.init(source, options, env);

    if (env.getIteratorScope() == IteratorScope.scan) {
      throw new IOException("This iterator not intended for use at scan time");
    }

    bulkTxStatusCache = new HashMap<>();
    arbitrator = getArbitrator(((SystemIteratorEnvironment) env).getServerContext());
  }

  protected Arbitrator getArbitrator(ServerContext context) {
    return new ZooArbitrator(context);
  }
}
