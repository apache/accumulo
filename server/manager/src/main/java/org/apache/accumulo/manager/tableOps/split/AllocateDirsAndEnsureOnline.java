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
package org.apache.accumulo.manager.tableOps.split;

import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.clientImpl.AcceptableThriftTableOperationException;
import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.clientImpl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletOperationId;
import org.apache.accumulo.core.metadata.schema.TabletOperationType;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.server.tablets.TabletNameGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AllocateDirsAndEnsureOnline extends ManagerRepo {

  private static final long serialVersionUID = 1L;
  private static final Logger log = LoggerFactory.getLogger(PreSplit.class);

  private final SplitInfo splitInfo;

  public AllocateDirsAndEnsureOnline(SplitInfo splitInfo) {
    this.splitInfo = splitInfo;
  }

  @Override
  public Repo<Manager> call(FateId fateId, Manager manager) throws Exception {
    // This check of table state is done after setting the operation id to avoid a race condition
    // with the client code that waits for a table to go offline. That client code sets the table
    // state and then scans the metadata table looking for split operations ids. If split checks
    // tables state before setting the opid then there is race condition with the client. Setting it
    // after ensures that in the case when the client does not see any split op id in the metadata
    // table that it knows that any splits starting after that point in time will not complete. This
    // order is needed because the split fate operation does not acquire a table lock in zookeeper.
    if (manager.getContext().getTableState(splitInfo.getOriginal().tableId())
        != TableState.ONLINE) {

      var opid = TabletOperationId.from(TabletOperationType.SPLITTING, fateId);

      // attempt to delete the operation id
      try (var tabletsMutator = manager.getContext().getAmple().conditionallyMutateTablets()) {

        Ample.RejectionHandler rejectionHandler = new Ample.RejectionHandler() {

          @Override
          public boolean callWhenTabletDoesNotExists() {
            return true;
          }

          @Override
          public boolean test(TabletMetadata tabletMetadata) {
            // if the tablet no longer exists or our operation id is not set then consider a success
            return tabletMetadata == null || !opid.equals(tabletMetadata.getOperationId());
          }
        };

        tabletsMutator.mutateTablet(splitInfo.getOriginal()).requireOperation(opid)
            .requireAbsentLocation().requireAbsentLogs().deleteOperation().submit(rejectionHandler);

        var result = tabletsMutator.process().get(splitInfo.getOriginal());

        if (result.getStatus() != Ample.ConditionalResult.Status.ACCEPTED) {
          throw new IllegalStateException(
              "Failed to delete operation id " + splitInfo.getOriginal());
        }
      }

      throw new AcceptableThriftTableOperationException(
          splitInfo.getOriginal().tableId().canonical(), null, TableOperation.SPLIT,
          TableOperationExceptionType.OFFLINE,
          "Unable to split tablet because the table is offline");
    } else {
      // Create the dir name here for the next step. If the next step fails it will always have the
      // same dir name each time it runs again making it idempotent.
      List<String> dirs = new ArrayList<>();

      splitInfo.getSplits().forEach(split -> {
        String dirName = TabletNameGenerator.createTabletDirectoryName(manager.getContext(), split);
        dirs.add(dirName);
        log.trace("{} allocated dir name {}", fateId, dirName);
      });
      return new UpdateTablets(splitInfo, dirs);
    }
  }
}
