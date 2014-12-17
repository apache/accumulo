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
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.fate.zookeeper.TransactionWatcher.Arbitrator;
import org.apache.accumulo.server.zookeeper.TransactionWatcher.ZooArbitrator;
import org.apache.log4j.Logger;

/**
 * A special iterator for the metadata table that removes inactive bulk load flags
 * 
 */
public class MetadataBulkLoadFilter extends Filter {
  private static Logger log = Logger.getLogger(MetadataBulkLoadFilter.class);
  
  enum Status {
    ACTIVE, INACTIVE
  }
  
  Map<Long,Status> bulkTxStatusCache;
  Arbitrator arbitrator;
  
  @Override
  public boolean accept(Key k, Value v) {
    if (!k.isDeleted() && k.compareColumnFamily(TabletsSection.BulkFileColumnFamily.NAME) == 0) {
      long txid = Long.valueOf(v.toString());
      
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
          log.error(e, e);
        }
        
        bulkTxStatusCache.put(txid, status);
      }
      
      return status == Status.ACTIVE;
    }
    
    return true;
  }
  
  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    
    if (env.getIteratorScope() == IteratorScope.scan) {
      throw new IOException("This iterator not intended for use at scan time");
    }
    
    bulkTxStatusCache = new HashMap<Long,MetadataBulkLoadFilter.Status>();
    arbitrator = getArbitrator();
  }
  
  protected Arbitrator getArbitrator() {
    return new ZooArbitrator();
  }
}
