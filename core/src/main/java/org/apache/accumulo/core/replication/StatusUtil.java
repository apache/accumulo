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
package org.apache.accumulo.core.replication;

import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.protobuf.ProtobufUtil;
import org.apache.accumulo.core.replication.proto.Replication.Status;
import org.apache.accumulo.core.replication.proto.Replication.Status.Builder;

/**
 * Helper methods to create Status protobuf messages
 */
public class StatusUtil {

  private static final Status NEW_REPLICATION_STATUS, CLOSED_REPLICATION_STATUS;
  private static final Value NEW_REPLICATION_STATUS_VALUE, CLOSED_REPLICATION_STATUS_VALUE;

  static {
    Status.Builder builder = Status.newBuilder();
    builder.setBegin(0);
    builder.setEnd(0);
    builder.setInfiniteEnd(false);
    builder.setClosed(false);
    NEW_REPLICATION_STATUS = builder.build();
    NEW_REPLICATION_STATUS_VALUE = ProtobufUtil.toValue(NEW_REPLICATION_STATUS);

    builder = Status.newBuilder();
    builder.setBegin(0);
    builder.setEnd(0);
    builder.setInfiniteEnd(true);
    builder.setClosed(true);
    CLOSED_REPLICATION_STATUS = builder.build();
    CLOSED_REPLICATION_STATUS_VALUE = ProtobufUtil.toValue(CLOSED_REPLICATION_STATUS);
  }

  /**
   * @return A {@link Status} which represents a file with no data that is open for writes
   */
  public static Status newFile() {
    return NEW_REPLICATION_STATUS;
  }

  /**
   * @return A {@link Value} which represent a file with no data that is open for writes
   */
  public static Value newFileValue() {
    return NEW_REPLICATION_STATUS_VALUE;
  }

  /**
   * Creates a {@link Status} for newly-created data that must be replicated
   * 
   * @param recordsIngested
   *          Offset of records which need to be replicated
   * @return A {@link Status} tracking data that must be replicated
   */
  public static Status ingestedUntil(long recordsIngested) {
    return ingestedUntil(Status.newBuilder(), recordsIngested);
  }

  public static Status ingestedUntil(Builder builder, long recordsIngested) {
    return replicatedAndIngested(builder, 0, recordsIngested);
  }

  /**
   * @param recordsReplicated
   *          Offset of records which have been replicated
   * @return A {@link Status} tracking data that must be replicated
   */
  public static Status replicated(long recordsReplicated) {
    return replicated(Status.newBuilder(), recordsReplicated);
  }

  /**
   * @param builder
   *          Existing {@link Builder} to use
   * @param recordsReplicated
   *          Offset of records which have been replicated
   * @returnA {@link Status} tracking data that must be replicated
   */
  public static Status replicated(Status.Builder builder, long recordsReplicated) {
    return replicatedAndIngested(builder, recordsReplicated, 0);
  }

  /**
   * Creates a @{link Status} for a file which has new data and data which has been replicated
   * 
   * @param recordsReplicated
   *          Offset of records which have been replicated
   * @param recordsIngested
   *          Offset for records which need to be replicated
   * @return A {@link Status} for the given parameters
   */
  public static Status replicatedAndIngested(long recordsReplicated, long recordsIngested) {
    return replicatedAndIngested(Status.newBuilder(), recordsReplicated, recordsIngested);
  }

  /**
   * Same as {@link #replicatedAndIngested(long, long)} but uses the provided {@link Builder}
   * 
   * @param builder
   *          An existing builder
   * @param recordsReplicated
   *          Offset of records which have been replicated
   * @param recordsIngested
   *          Offset of records which need to be replicated
   * @return A {@link Status} for the given parameters using the builder
   */
  public static Status replicatedAndIngested(Status.Builder builder, long recordsReplicated, long recordsIngested) {
    return builder.setBegin(recordsReplicated).setEnd(recordsIngested).setClosed(false).setInfiniteEnd(false).build();
  }

  /**
   * @return A {@link Status} for a closed file of unspecified length, all of which needs replicating.
   */
  public static Status fileClosed() {
    return CLOSED_REPLICATION_STATUS;
  }

  /**
   * @return A {@link Value} for a closed file of unspecified length, all of which needs replicating.
   */
  public static Value fileClosedValue() {
    return CLOSED_REPLICATION_STATUS_VALUE;
  }

  /**
   * Is the given Status fully replicated and is its file ready for deletion on the source
   * @param status a Status protobuf
   * @return True if the file this Status references can be deleted.
   */
  public static boolean isCompletelyReplicated(Status status) {
    if (status.getClosed()) {
      if (status.getInfiniteEnd()) {
        return Long.MAX_VALUE == status.getBegin();
      } else {
        return status.getBegin() == status.getEnd();
      }
    }

    return false;
  }
}
