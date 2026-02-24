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
package org.apache.accumulo.server.util.bulkCommand;

import java.time.Duration;
import java.time.Instant;
import java.util.EnumSet;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.apache.accumulo.core.cli.ServerOpts;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.Fate;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.ReadOnlyFateStore;
import org.apache.accumulo.core.fate.ReadOnlyFateStore.FateIdStatus;
import org.apache.accumulo.core.fate.TraceRepo;
import org.apache.accumulo.core.fate.user.UserFateStore;
import org.apache.accumulo.core.metadata.SystemTables;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.util.ServerKeywordExecutable;
import org.apache.accumulo.start.spi.CommandGroup;
import org.apache.accumulo.start.spi.CommandGroups;

import com.beust.jcommander.JCommander;

public class ListBulk extends ServerKeywordExecutable<ServerOpts> {

  public ListBulk() {
    super(new ServerOpts());
  }

  @Override
  public String keyword() {
    return "list-bulk";
  }

  @Override
  public CommandGroup commandGroup() {
    return CommandGroups.INSTANCE;
  }

  @Override
  public String description() {
    return "List current bulk import operations";
  }

  @Override
  public void execute(JCommander cl, ServerOpts options) throws Exception {
    ServerContext context = getServerContext();
    System.out.printf(" %-46s | %-8s | %-10s | %-7s | %-20s | %-20s\n", "FATE ID", "TABLE ID",
        "STATE", "AGE", "SOURCE DIR", "DESTINATION DIR");
    var now = Instant.now();
    list(context, bs -> {
      var elapsed = Duration.between(bs.lastUpdate, now);
      System.out.printf(" %46s | %8s | %10s | %d:%02d:%02d | %20s | %20s\n", bs.fateId, bs.tableId,
          bs.state, elapsed.toHours(), elapsed.toMinutesPart(), elapsed.toSecondsPart(),
          bs.sourceDir, bs.destDir);
    });
  }

  public enum BulkState {
    PREPARING, MOVING, LOADING, REFRESHING, CLEANING
  }

  public record BulkStatus(FateId fateId, TableId tableId, String sourceDir, String destDir,
      Instant lastUpdate, BulkState state) {
  }

  /**
   * Gathers summary information about all running bulk import operations.
   */
  public static void list(ServerContext context, Consumer<BulkStatus> statusConsumer) {
    var fateStore = new UserFateStore<>(context, SystemTables.FATE.tableName(), null, null);

    try (var fateStream = fateStore.list(
        EnumSet.of(ReadOnlyFateStore.TStatus.SUBMITTED, ReadOnlyFateStore.TStatus.IN_PROGRESS))) {
      fateStream
          .filter(fis -> fis.getFateOperation()
              .map(op -> op == Fate.FateOperation.TABLE_BULK_IMPORT2).orElse(false))
          .map(FateIdStatus::getFateId).flatMap(fateId -> getStatus(fateStore, fateId))
          .forEach(statusConsumer);
    }
  }

  private static Stream<BulkStatus> getStatus(UserFateStore<Object> fateStore, FateId fateId) {
    var txStore = fateStore.read(fateId);
    var top = txStore.top();
    if (top == null) {
      return Stream.of();
    }

    BulkFateOperation bfo;
    if (top instanceof TraceRepo<Object>) {
      var wrapped = ((TraceRepo<Object>) top).getWrapped();
      if (wrapped instanceof BulkFateOperation) {
        bfo = (BulkFateOperation) wrapped;
      } else {
        throw new IllegalStateException(
            "Unknown fate operation class type " + wrapped.getClass().getName());
      }
    } else if (top instanceof BulkFateOperation) {
      bfo = (BulkFateOperation) top;
    } else {
      throw new IllegalStateException(
          "Unknown fate operation class type " + top.getClass().getName());
    }

    return Stream.of(new ListBulk.BulkStatus(fateId, bfo.getTableId(), bfo.getSourceDir(),
        bfo.getDestDir(), bfo.getCreationTime(), bfo.getState()));
  }

}
