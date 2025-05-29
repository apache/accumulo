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
package org.apache.accumulo.test.compaction;

import org.apache.accumulo.coordinator.CompactionCoordinator;
import org.apache.accumulo.coordinator.CompactionFinalizer;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.compaction.thrift.CompactionCoordinatorService;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionFinalState;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionFinalState.FinalState;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.process.thrift.ServerProcessService;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.ServerOpts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestCompactionCoordinatorForOfflineTable extends CompactionCoordinator
    implements CompactionCoordinatorService.Iface, ServerProcessService.Iface {

  public static class NonNotifyingCompactionFinalizer extends CompactionFinalizer {

    private static final Logger LOG =
        LoggerFactory.getLogger(NonNotifyingCompactionFinalizer.class);

    NonNotifyingCompactionFinalizer(ServerContext context) {
      super(context, context.getScheduledExecutor());
    }

    @Override
    public void commitCompaction(ExternalCompactionId ecid, KeyExtent extent, long fileSize,
        long fileEntries) {

      var ecfs = new ExternalCompactionFinalState(ecid, extent, FinalState.FINISHED, fileSize,
          fileEntries);

      // write metadata entry
      LOG.info("Writing completed external compaction to metadata table: {}", ecfs);
      try (BatchWriter writer = context.createBatchWriter(Ample.DataLevel.USER.metaTable())) {
        writer.addMutation(ecfs.toMutation());
      } catch (MutationsRejectedException | TableNotFoundException e) {
        throw new RuntimeException(e);
      }

      // queue RPC if queue is not full
      LOG.info("Skipping tserver notification for completed external compaction: {}", ecfs);
    }

  }

  protected TestCompactionCoordinatorForOfflineTable(ServerOpts opts, String[] args) {
    super(opts, args);
  }

  @Override
  protected CompactionFinalizer createCompactionFinalizer() {
    return new NonNotifyingCompactionFinalizer(getContext());
  }

  public static void main(String[] args) throws Exception {
    try (TestCompactionCoordinatorForOfflineTable coordinator =
        new TestCompactionCoordinatorForOfflineTable(new ServerOpts(), args)) {
      coordinator.runServer();
    }
  }
}
