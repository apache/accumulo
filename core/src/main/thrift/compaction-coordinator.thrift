/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
namespace java org.apache.accumulo.core.compaction.thrift
namespace cpp org.apache.accumulo.core.compaction.thrift

include "data.thrift"
include "security.thrift"
include "tabletserver.thrift"
include "trace.thrift"

enum Action {
  ADD
  UPDATE
  CANCEL
}

enum CompactionState {
  # Coordinator should set state to ASSIGNED when getCompactionJob is called by Compactor
  ASSIGNED
  # Compactor should set state to STARTED when compaction has successfully begun
  STARTED
  # Compactor can call repeatedly with an updated message to reflect percentage complete
  IN_PROGRESS
  # Compactor should set state to SUCCEEDED when compaction job has successfully finished
  SUCCEEDED
  # Compactor should set state to FAILED when compaction job fails, message should be mandatory
  FAILED
  # Compactor should set state to CANCELLED to acknowledge that it has stopped compacting 
  CANCELLED
}

struct CompactionJob {
  1:trace.TInfo traceInfo
  2:security.TCredentials credentials
  3:i64 compactionId
  5:data.TKeyExtent extent
  6:list<string> files
  7:i32 priority
  8:i32 readRate
  9:i32 writeRate
  10:tabletserver.IteratorConfig iteratorSettings
  11:tabletserver.CompactionType type
  # Need to add SELECTOR To CompactionReason, delete CompactionKind?
  12:tabletserver.CompactionReason reason
  13:string outputFile
}

struct Status {
  1:i64 timestamp
  2:i64 compactionId
  3:string compactorHostAndPort
  4:CompactionState state
  5:string message
}

service CompactionCoordinator {

  /*
   * Called by TabletServer (or CLI) to add, update, or cancel a compaction for a tablet
   */
  void AddOrUpdateCompaction(
    1:data.TKeyExtent extent
    2:string queueName
    3:i64 priority
    4:Action action
  )
  
  /*
   * Called by TabletServer (or CLI) to get current status of compaction for a tablet
   */
  Status getCompactionStatus(
    1:string tableId
    2:string endRow
    3:string queueName
  )
  
  /*
   * Called by TabletServer (or CLI) to get complete status of compaction for a tablet
   */
  list<Status> getCompactionStatusHistory(
    1:string tableId
    2:string endRow
    3:string queueName    
  )

  /*
   * Called by Compactor to get the next compaction job
   */
  CompactionJob getCompactionJob(
    1:string queueName
    2:i64 compactorHostAndPort
  )
  
  /*
   * Called by Compactor to update the Coordinator with the state of the compaction
   */
  void updateCompactionState(
    1:CompactionJob compaction
    2:CompactionState state
    3:string message
    4:i64 timestamp
  )

}

service Compactor {

  /*
   * Called by Coordinator to instruct the Compactor to stop working on the compaction.
   * Compactor should call updateCompcationState with state CANCELLED
   *
   * Question: What if Compactor is no longer working on job?
   */
  void cancel(
    1:CompactionJob compaction
  )

}