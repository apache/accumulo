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
package org.apache.accumulo.tserver.tablet;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.sample.impl.SamplerConfigurationImpl;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.tserver.InMemoryMap;
import org.apache.accumulo.tserver.InMemoryMap.MemoryIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TabletMemory implements Closeable {
  private static final Logger log = LoggerFactory.getLogger(TabletMemory.class);

  private final Tablet tablet;
  private InMemoryMap memTable;
  private InMemoryMap otherMemTable;
  private InMemoryMap deletingMemTable;
  private long nextSeq = 1L;
  private CommitSession commitSession;
  private ServerContext context;

  TabletMemory(Tablet tablet) {
    this.tablet = tablet;
    this.context = tablet.getContext();
    memTable =
        new InMemoryMap(tablet.getTableConfiguration(), context, tablet.getExtent().tableId());
    commitSession = new CommitSession(tablet, nextSeq, memTable);
    nextSeq += 2;
  }

  public InMemoryMap getMemTable() {
    return memTable;
  }

  public InMemoryMap getMinCMemTable() {
    return otherMemTable;
  }

  public CommitSession prepareForMinC() {
    if (otherMemTable != null) {
      throw new IllegalStateException();
    }

    if (deletingMemTable != null) {
      throw new IllegalStateException();
    }
    if (commitSession == null) {
      throw new IllegalStateException();
    }

    otherMemTable = memTable;
    memTable =
        new InMemoryMap(tablet.getTableConfiguration(), context, tablet.getExtent().tableId());

    CommitSession oldCommitSession = commitSession;
    commitSession = new CommitSession(tablet, nextSeq, memTable);
    nextSeq += 2;

    tablet.updateMemoryUsageStats(memTable.estimatedSizeInBytes(),
        otherMemTable.estimatedSizeInBytes());

    return oldCommitSession;
  }

  public void finishedMinC() {

    if (otherMemTable == null) {
      throw new IllegalStateException();
    }

    if (deletingMemTable != null) {
      throw new IllegalStateException();
    }

    if (commitSession == null) {
      throw new IllegalStateException();
    }

    deletingMemTable = otherMemTable;

    otherMemTable = null;
    tablet.notifyAll();
  }

  public void finalizeMinC() {
    if (commitSession == null) {
      throw new IllegalStateException();
    }
    try {
      deletingMemTable.delete(15000);
    } finally {
      synchronized (tablet) {
        if (otherMemTable != null) {
          throw new IllegalStateException();
        }

        if (deletingMemTable == null) {
          throw new IllegalStateException();
        }

        deletingMemTable = null;

        tablet.updateMemoryUsageStats(memTable.estimatedSizeInBytes(), 0);
      }
    }
  }

  public boolean memoryReservedForMinC() {
    return otherMemTable != null || deletingMemTable != null;
  }

  public void waitForMinC() {
    while (otherMemTable != null || deletingMemTable != null) {
      try {
        tablet.wait(50);
      } catch (InterruptedException e) {
        log.warn("{}", e.getMessage(), e);
      }
    }
  }

  public void mutate(CommitSession cm, List<Mutation> mutations, int count) {
    cm.mutate(mutations, count);
  }

  public void updateMemoryUsageStats() {
    long other = 0;
    if (otherMemTable != null) {
      other = otherMemTable.estimatedSizeInBytes();
    } else if (deletingMemTable != null) {
      other = deletingMemTable.estimatedSizeInBytes();
    }

    tablet.updateMemoryUsageStats(memTable.estimatedSizeInBytes(), other);
  }

  public List<MemoryIterator> getIterators(SamplerConfigurationImpl samplerConfig) {
    List<MemoryIterator> toReturn = new ArrayList<>(2);
    toReturn.add(memTable.skvIterator(samplerConfig));
    if (otherMemTable != null) {
      toReturn.add(otherMemTable.skvIterator(samplerConfig));
    }
    return toReturn;
  }

  public void returnIterators(List<MemoryIterator> iters) {
    for (MemoryIterator iter : iters) {
      iter.close();
    }
  }

  public long getNumEntries() {
    if (otherMemTable != null) {
      return memTable.getNumEntries() + otherMemTable.getNumEntries();
    }
    return memTable.getNumEntries();
  }

  public CommitSession getCommitSession() {
    return commitSession;
  }

  @Override
  public void close() {
    commitSession = null;
  }
}
