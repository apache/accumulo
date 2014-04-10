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

import org.apache.accumulo.core.replication.proto.Replication.ReplicationStatus;

/**
 * Helper methods to create ReplicationStatus protobuf messages
 */
public class ReplicationStatusUtil {

  private static final ReplicationStatus NEW_REPLICATION_STATUS, CLOSED_REPLICATION_STATUS;
  
  static {
    ReplicationStatus.Builder builder = ReplicationStatus.newBuilder();
    builder.setBegin(0);
    builder.setEnd(0);
    builder.setClosed(false);
    NEW_REPLICATION_STATUS = builder.build();
    
    builder = ReplicationStatus.newBuilder();
    builder.setBegin(0);
    builder.setEnd(0);
    builder.setClosed(true);
    CLOSED_REPLICATION_STATUS = builder.build();
  }

  /**
   * @return A {@link ReplicationStatus} which represents a file with no data that is open for writes
   */
  public static ReplicationStatus newFile() {
    return NEW_REPLICATION_STATUS;
  }

  /**
   * Creates a {@link ReplicationStatus} for newly-created data that must be replicated
   * @param recordsIngested Offset of records which are ready for replication
   * @return A {@link ReplicationStatus} tracking data that must be replicated
   */
  public static ReplicationStatus newData(long recordsIngested) {
    return ReplicationStatus.newBuilder().setBegin(0).setEnd(recordsIngested).setClosed(false).build();
  }

  /**
   * @param recordsReplicated Offset of records where replication has occurred
   * @return A {@link ReplicationStatus} tracking data that has been replicated
   */
  public static ReplicationStatus dataReplicated(long recordsReplicated) {
    return ReplicationStatus.newBuilder().setBegin(recordsReplicated).setEnd(0).setClosed(false).build();
  }

  /**
   * @return A {@link ReplicationStatus} 
   */
  public static ReplicationStatus fileClosed() {
    return CLOSED_REPLICATION_STATUS;
  }
}
