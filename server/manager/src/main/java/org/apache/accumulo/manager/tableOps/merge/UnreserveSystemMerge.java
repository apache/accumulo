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
package org.apache.accumulo.manager.tableOps.merge;

import org.apache.accumulo.core.clientImpl.AcceptableThriftTableOperationException;
import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.clientImpl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.manager.merge.FindMergeableRangeTask.UnmergeableReason;
import org.apache.accumulo.manager.tableOps.AbstractFateOperation;
import org.apache.accumulo.manager.tableOps.FateEnv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UnreserveSystemMerge extends AbstractFateOperation {

  private static final long serialVersionUID = 1L;
  private static final Logger log = LoggerFactory.getLogger(UnreserveSystemMerge.class);
  private final MergeInfo mergeInfo;
  private final long maxFileCount;
  private final long maxTotalSize;
  private final UnmergeableReason reason;

  public UnreserveSystemMerge(MergeInfo mergeInfo, UnmergeableReason reason, long maxFileCount,
      long maxTotalSize) {
    this.mergeInfo = mergeInfo;
    this.reason = reason;
    this.maxFileCount = maxFileCount;
    this.maxTotalSize = maxTotalSize;
  }

  @Override
  public Repo<FateEnv> call(FateId fateId, FateEnv environment) throws Exception {
    FinishTableRangeOp.removeOperationIds(log, mergeInfo, fateId, environment);
    throw new AcceptableThriftTableOperationException(mergeInfo.tableId.toString(), null,
        mergeInfo.op.isMergeOp() ? TableOperation.MERGE : TableOperation.DELETE_RANGE,
        TableOperationExceptionType.OTHER, formatReason());
  }

  public UnmergeableReason getReason() {
    return reason;
  }

  private String formatReason() {
    return switch (reason) {
      case MAX_FILE_COUNT ->
        "Aborted merge because it would produce a tablet with more files than the configured limit of "
            + maxFileCount;
      case MAX_TOTAL_SIZE ->
        "Aborted merge because it would produce a tablet with a file size larger than the configured limit of "
            + maxTotalSize;
      // This state should not happen as VerifyMergeability repo checks consistency but adding it
      // just in case
      case TABLET_MERGEABILITY ->
        "Aborted merge because one ore more tablets in the merge range are unmergeable.";
      case NOT_CONTIGUOUS ->
        "Aborted merge because the tablets in a range do not form a linked list.";
    };

  }
}
