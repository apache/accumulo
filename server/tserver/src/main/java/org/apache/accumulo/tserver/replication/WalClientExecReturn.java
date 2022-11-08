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
package org.apache.accumulo.tserver.replication;

import java.io.DataInputStream;
import java.util.Set;

import org.apache.accumulo.core.replication.ReplicationTarget;
import org.apache.accumulo.core.replication.thrift.ReplicationServicer;
import org.apache.accumulo.core.replication.thrift.ReplicationServicer.Client;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes.Exec;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.server.replication.proto.Replication.Status;
import org.apache.hadoop.fs.Path;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Deprecated
class WalClientExecReturn implements Exec<ReplicationStats,ReplicationServicer.Client> {

  private static final Logger log = LoggerFactory.getLogger(WalClientExecReturn.class);

  private final AccumuloReplicaSystem ars;
  private ReplicationTarget target;
  private DataInputStream input;
  private Path p;
  private Status status;
  private long sizeLimit;
  private String remoteTableId;
  private TCredentials tcreds;
  private Set<Integer> tids;

  public WalClientExecReturn(AccumuloReplicaSystem ars, ReplicationTarget target,
      DataInputStream input, Path p, Status status, long sizeLimit, String remoteTableId,
      TCredentials tcreds, Set<Integer> tids) {
    this.ars = ars;
    this.target = target;
    this.input = input;
    this.p = p;
    this.status = status;
    this.sizeLimit = sizeLimit;
    this.remoteTableId = remoteTableId;
    this.tcreds = tcreds;
    this.tids = tids;
  }

  @Override
  public ReplicationStats execute(Client client) throws TException {
    WalReplication edits = ars.getWalEdits(target, input, p, status, sizeLimit, tids);

    log.debug(
        "Read {} WAL entries and retained {} bytes of WAL entries for replication to peer '{}'",
        (edits.entriesConsumed == Long.MAX_VALUE) ? "all remaining" : edits.entriesConsumed,
        edits.sizeInBytes, p);

    // If we have some edits to send
    if (edits.walEdits.getEditsSize() > 0) {
      log.debug("Sending {} edits", edits.walEdits.getEditsSize());
      long entriesReplicated = client.replicateLog(remoteTableId, edits.walEdits, tcreds);
      if (entriesReplicated == edits.numUpdates) {
        log.debug("Replicated {} edits", entriesReplicated);
      } else {
        log.warn("Sent {} WAL entries for replication but {} were reported as replicated",
            edits.numUpdates, entriesReplicated);
      }

      // We don't have to replicate every LogEvent in the file (only Mutation LogEvents), but we
      // want to track progress in the file relative to all LogEvents (to avoid duplicative
      // processing/replication)
      return edits;
    } else if (edits.entriesConsumed > 0) {
      // Even if we send no data, we want to record a non-zero new begin value to avoid checking
      // the same
      // log entries multiple times to determine if they should be sent
      return edits;
    }

    // No data sent (bytes nor records) and no progress made
    return new ReplicationStats(0L, 0L, 0L);
  }
}
