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

include "client.thrift"
include "data.thrift"
include "security.thrift"
include "tabletserver.thrift"
include "trace.thrift"

enum TCompactionState {
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

struct Status {
  1:i64 timestamp
  2:string externalCompactionId
  3:string compactor
  4:TCompactionState state
  5:string message
}

exception UnknownCompactionIdException {}

service CompactionCoordinatorService {

  /*
   * Called by Compactor on successful completion of compaction job
   */
  void compactionCompleted(
    1:trace.TInfo tinfo
    2:security.TCredentials credentials  
    3:string externalCompactionId
    4:data.TKeyExtent extent
    5:tabletserver.TCompactionStats stats
  )
  
  /*
   * Called by Compactor to get the next compaction job
   */
  tabletserver.TExternalCompactionJob getCompactionJob(
    1:trace.TInfo tinfo
    2:security.TCredentials credentials
    3:string queueName
    4:string compactor
    5:string externalCompactionId
  )
  
  /*
   * Called by Compactor to update the Coordinator with the state of the compaction
   */
  void updateCompactionStatus(
    1:trace.TInfo tinfo
    2:security.TCredentials credentials
    3:string externalCompactionId
    4:TCompactionState state
    5:string message
    6:i64 timestamp
  )
  
  /*
   * Called by Compactor on unsuccessful completion of compaction job
   */
  void compactionFailed(
    1:trace.TInfo tinfo
    2:security.TCredentials credentials
    3:string externalCompactionId
    4:data.TKeyExtent extent
  )

}

service CompactorService {

  /*
   * Called by Coordinator to instruct the Compactor to stop working on the compaction.
   */
  void cancel(
    1:trace.TInfo tinfo
    2:security.TCredentials credentials
    3:string externalCompactionId
  ) throws (
    1:UnknownCompactionIdException e
  )

  tabletserver.TExternalCompactionJob getRunningCompaction(
    1:trace.TInfo tinfo
    2:security.TCredentials credentials
  ) throws (
    1:client.ThriftSecurityException sec
  )

  string getRunningCompactionId(
    1:trace.TInfo tinfo
    2:security.TCredentials credentials
  ) throws (
    1:client.ThriftSecurityException sec
  )

  list<tabletserver.ActiveCompaction> getActiveCompactions(
    2:trace.TInfo tinfo
    1:security.TCredentials credentials
  ) throws (
    1:client.ThriftSecurityException sec
  )
}
