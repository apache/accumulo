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
package org.apache.accumulo.server.replication;

import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.protobuf.ProtobufUtil;
import org.apache.accumulo.server.replication.proto.Replication.Status;
import org.apache.accumulo.server.replication.proto.Replication.Status.Builder;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Helper methods to create Status protobuf messages
 */
public class StatusUtil {

  private static final Status INF_END_REPLICATION_STATUS, CLOSED_STATUS;
  private static final Value INF_END_REPLICATION_STATUS_VALUE, CLOSED_STATUS_VALUE;

  private static final Status.Builder CREATED_STATUS_BUILDER;

  static {
    CREATED_STATUS_BUILDER = Status.newBuilder();
    CREATED_STATUS_BUILDER.setBegin(0);
    CREATED_STATUS_BUILDER.setEnd(0);
    CREATED_STATUS_BUILDER.setInfiniteEnd(false);
    CREATED_STATUS_BUILDER.setClosed(false);

    Builder builder = Status.newBuilder();
    builder.setBegin(0);
    builder.setEnd(0);
    builder.setInfiniteEnd(true);
    builder.setClosed(false);
    INF_END_REPLICATION_STATUS = builder.build();
    INF_END_REPLICATION_STATUS_VALUE = ProtobufUtil.toValue(INF_END_REPLICATION_STATUS);

    builder = Status.newBuilder();
    builder.setBegin(0);
    builder.setEnd(0);
    builder.setInfiniteEnd(true);
    builder.setClosed(true);
    CLOSED_STATUS = builder.build();
    CLOSED_STATUS_VALUE = ProtobufUtil.toValue(CLOSED_STATUS);
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
   * @return A {@link Status} tracking data that must be replicated
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
   * @return A {@link Status} for a new file that was just created
   */
  public static synchronized Status fileCreated(long timeCreated) {
    // We're using a shared builder, so we need to synchronize access on it until we make a Status (which is then immutable)
    CREATED_STATUS_BUILDER.setCreatedTime(timeCreated);
    return CREATED_STATUS_BUILDER.build();
  }

  /**
   * @return A {@link Value} for a new file that was just created
   */
  public static Value fileCreatedValue(long timeCreated) {
    return ProtobufUtil.toValue(fileCreated(timeCreated));
  }

  /**
   * @return A Status representing a closed file
   */
  public static Status fileClosed() {
    return CLOSED_STATUS;
  }

  /**
   * @return A Value representing a closed file
   */
  public static Value fileClosedValue() {
    return CLOSED_STATUS_VALUE;
  }

  /**
   * @return A {@link Status} for an open file of unspecified length, all of which needs replicating.
   */
  public static Status openWithUnknownLength(long timeCreated) {
    Builder builder = Status.newBuilder();
    builder.setBegin(0);
    builder.setEnd(0);
    builder.setInfiniteEnd(true);
    builder.setClosed(false);
    builder.setCreatedTime(timeCreated);
    return builder.build();
  }

  /**
   * @return A {@link Status} for an open file of unspecified length, all of which needs replicating.
   */
  public static Status openWithUnknownLength() {
    return INF_END_REPLICATION_STATUS;
  }

  /**
   * @return A {@link Value} for an open file of unspecified length, all of which needs replicating.
   */
  public static Value openWithUnknownLengthValue() {
    return INF_END_REPLICATION_STATUS_VALUE;
  }

  /**
   * @param v
   *          Value with serialized Status
   * @return A Status created from the Value
   */
  public static Status fromValue(Value v) throws InvalidProtocolBufferException {
    return Status.parseFrom(v.get());
  }

  /**
   * Is the given Status fully replicated and is its file ready for deletion on the source
   *
   * @param status
   *          a Status protobuf
   * @return True if the file this Status references can be deleted.
   */
  public static boolean isSafeForRemoval(Status status) {
    return status.getClosed() && isFullyReplicated(status);
  }

  /**
   * Is the given Status fully replicated but potentially not yet safe for deletion
   *
   * @param status
   *          a Status protobuf
   * @return True if the file this Status references is fully replicated so far
   */
  public static boolean isFullyReplicated(Status status) {
    if (status.getInfiniteEnd()) {
      return Long.MAX_VALUE == status.getBegin();
    } else {
      return status.getBegin() >= status.getEnd();
    }
  }

  /**
   * Given the {@link Status}, is there replication work to be done
   *
   * @param status
   *          Status for a file
   * @return true if replication work is required
   */
  public static boolean isWorkRequired(Status status) {
    if (status.getInfiniteEnd()) {
      return Long.MAX_VALUE != status.getBegin();
    } else {
      return status.getBegin() < status.getEnd();
    }
  }
}
