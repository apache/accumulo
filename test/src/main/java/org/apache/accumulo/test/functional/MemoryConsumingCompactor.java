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
package org.apache.accumulo.test.functional;

import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.compactor.Compactor;
import org.apache.accumulo.core.cli.ConfigOpts;
import org.apache.accumulo.core.clientImpl.thrift.TInfo;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.compaction.thrift.CompactorService;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.core.tabletserver.thrift.TExternalCompactionJob;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.iterators.SystemIteratorEnvironment;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemoryConsumingCompactor extends Compactor implements CompactorService.Iface {

  private static final Logger LOG = LoggerFactory.getLogger(MemoryConsumingCompactor.class);

  MemoryConsumingCompactor(ConfigOpts opts, String[] args) {
    super(opts, args);
  }

  @Override
  protected CompactorService.Iface getCompactorThriftHandlerInterface() {
    return this;
  }

  @Override
  public void cancel(TInfo tinfo, TCredentials credentials, String externalCompactionId)
      throws TException {
    // Use the cancel Thrift RPC to free the consumed memory
    LOG.warn("cancel called, freeing memory");
    MemoryConsumingIterator.freeBuffers();
  }

  @Override
  public TExternalCompactionJob getRunningCompaction(TInfo tinfo, TCredentials credentials)
      throws ThriftSecurityException, TException {
    // Use the getRunningCompaction Thrift RPC to consume the memory
    LOG.warn("getRunningCompaction called, consuming memory");
    try {
      MemoryConsumingIterator iter = new MemoryConsumingIterator();
      iter.init((SortedKeyValueIterator<Key,Value>) null, Map.of(),
          new SystemIteratorEnvironment() {

            @Override
            public ServerContext getServerContext() {
              return getContext();
            }

            @Override
            public SortedKeyValueIterator<Key,Value>
                getTopLevelIterator(SortedKeyValueIterator<Key,Value> iter) {
              return null;
            }

          });
      iter.consume();
    } catch (IOException e) {
      throw new TException("Error consuming memory", e);
    }
    return new TExternalCompactionJob();
  }

  public static void main(String[] args) throws Exception {
    try (var compactor = new MemoryConsumingCompactor(new ConfigOpts(), args)) {
      compactor.runServer();
    }
  }

}
