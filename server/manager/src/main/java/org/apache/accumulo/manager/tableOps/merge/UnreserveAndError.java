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
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UnreserveAndError extends ManagerRepo {
  public enum Reason {
    MAX_FILES, MAX_SIZE, UNMERGEABLE
  }

  private static final long serialVersionUID = 1L;
  private static final Logger log = LoggerFactory.getLogger(UnreserveAndError.class);
  private final MergeInfo mergeInfo;
  private final long total;
  private final long max;
  private final Reason reason;

  public UnreserveAndError(MergeInfo mergeInfo, long totalFiles, long maxFiles) {
    this(mergeInfo, Reason.MAX_FILES, totalFiles, maxFiles);
  }

  public UnreserveAndError(MergeInfo mergeInfo, long unmergeable) {
    this.mergeInfo = mergeInfo;
    this.reason = Reason.UNMERGEABLE;
    this.total = unmergeable;
    this.max = 0;
  }

  public UnreserveAndError(MergeInfo mergeInfo, Reason reason, long total, long max) {
    this.mergeInfo = mergeInfo;
    this.reason = reason;
    this.total = total;
    this.max = max;
  }

  @Override
  public Repo<Manager> call(FateId fateId, Manager environment) throws Exception {
    FinishTableRangeOp.removeOperationIds(log, mergeInfo, fateId, environment);
    throw new AcceptableThriftTableOperationException(mergeInfo.tableId.toString(), null,
        mergeInfo.op.isMergeOp() ? TableOperation.MERGE : TableOperation.DELETE_RANGE,
        TableOperationExceptionType.OTHER, formatReason());
  }

  public Reason getReason() {
    return reason;
  }

  private String formatReason() {
    switch (reason) {
      case MAX_FILES:
        return "Aborted merge because it would produce a tablets with more files than the configured limit of "
            + max + ". Observed " + total + " files in the merge range.";
      case MAX_SIZE:
        return "Aborted merge because it would produce a tablets with a file size larger than the configured limit of "
            + max + ". Observed " + total + " file size in the merge range.";
      case UNMERGEABLE:
        return "Aborted merge because one ore more tablets in the merge range are unmergeable. "
            + "Observed " + total + " unmergeable tablets in the merge range.";
      default:
        throw new IllegalArgumentException("Unknown Reason");
    }

  }
}
