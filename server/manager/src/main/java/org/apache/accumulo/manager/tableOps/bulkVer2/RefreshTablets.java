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
package org.apache.accumulo.manager.tableOps.bulkVer2;

import java.util.Map;

import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.fate.FateTxId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.tabletserver.thrift.TabletServerClientService;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.net.HostAndPort;

public class RefreshTablets extends ManagerRepo {

  private static final Logger log = LoggerFactory.getLogger(RefreshTablets.class);

  private static final long serialVersionUID = 1L;

  private final BulkInfo bulkInfo;

  public RefreshTablets(BulkInfo bulkInfo) {
    this.bulkInfo = bulkInfo;
  }

  public long isReady(long tid, Manager manager) throws Exception {
    int refreshIdsSeen = 0;

    // ELASTICITY_TODO limit tablets scanned to range of bulk import extents
    try (
        var tablets = manager.getContext().getAmple().readTablets().forTable(bulkInfo.tableId)
            .checkConsistency().fetch(ColumnType.LOCATION, ColumnType.PREV_ROW, ColumnType.REFRESH)
            .build();
        var tabletsMutator = manager.getContext().getAmple().conditionallyMutateTablets()) {

      for (TabletMetadata tablet : tablets) {
        Map<Long,TServerInstance> refreshIds = tablet.getRefreshIds();

        if (tablet.getRefreshIds().containsKey(tid)) {
          var currInstance =
              tablet.getLocation() == null ? null : tablet.getLocation().getServerInstance();
          if (refreshIds.get(tid).equals(currInstance)) {
            // the same tserver is still hosting the tablet as when the refresh column was set
            // earlier, so need to send that tserver a request via RPC to refresh
            var server = tablet.getLocation().getHostAndPort();
            sendRefreshRequest(tid, manager, tablet, server);
          } else {
            // the tserver that was hosting the tablet when the refresh column was set is no longer
            // hosting, so any new tserver should see the updated data. Can delete the refresh
            // request.
            // ELASTICITY_TODO the code here assumes that a tablet reads its metadata after setting
            // its location.. need to ensure that is eventually true
            tabletsMutator.mutateTablet(tablet.getExtent()).requireAbsentOperation()
                .deleteRefreshId(tid).submit();
          }

          refreshIdsSeen++;
        }
      }

      tabletsMutator.process().forEach((extent, condResult) -> {
        if (condResult.getStatus() != ConditionalWriter.Status.ACCEPTED) {
          var metadata = condResult.readMetadata();
          log.debug("Tablet update failed {} {} {} {} ", FateTxId.formatTid(tid), extent,
              condResult.getStatus(), metadata.getOperationId());
        }
      });
    }

    if (refreshIdsSeen > 0) {
      return 1000;
    } else {
      return 0;
    }
  }

  private static void sendRefreshRequest(long tid, Manager manager, TabletMetadata tablet,
      HostAndPort server) {
    TabletServerClientService.Client client = null;
    try {
      var timeInMillis = manager.getConfiguration().getTimeInMillis(Property.MANAGER_BULK_TIMEOUT);
      client = ThriftUtil.getClient(ThriftClientTypes.TABLET_SERVER,
          tablet.getLocation().getHostAndPort(), manager.getContext(), timeInMillis);
      client.refreshTablet(TraceUtil.traceInfo(), manager.getContext().rpcCreds(),
          tablet.getExtent().toThrift(), tid);
    } catch (TException ex) {
      var fmtTid = FateTxId.formatTid(tid);
      log.debug("rpc failed server: " + server + ", " + fmtTid + " " + ex.getMessage(), ex);
    } finally {
      ThriftUtil.returnClient(client, manager.getContext());
    }
  }

  @Override
  public Repo<Manager> call(long tid, Manager environment) throws Exception {
    return new CleanUpBulkImport(bulkInfo);
  }
}
