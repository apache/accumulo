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
namespace java org.apache.accumulo.core.tasks.thrift
namespace cpp org.apache.accumulo.core.tasks.thrift

include "security.thrift"
include "client.thrift"

struct TaskRunnerInfo {
 1:string hostname
 2:i32 port
 3:string resourceGroup
}

struct TaskObject {
  1:string taskManager
  2:string taskID
  3:string objectType
  4:binary cborEncodedObject
}

struct TaskList {
  1:list<TaskObject> tasks
}

service TaskManager {

  TaskObject getTask(
    1:client.TInfo tinfo
    2:security.TCredentials credentials  
    3:TaskRunnerInfo taskRunner
    4:string taskID  
  )

  oneway void taskStatus(
    1:client.TInfo tinfo
    2:security.TCredentials credentials
    3:i64 timestamp
    4:TaskObject taskUpdateObject
  )

  void taskCompleted(
    1:client.TInfo tinfo
    2:security.TCredentials credentials  
    3:TaskObject task  
  )

  void taskFailed(
    1:client.TInfo tinfo
    2:security.TCredentials credentials  
    3:TaskObject task  
  )
  
  TaskList getCompletedTasks(
    1:client.TInfo tinfo
    2:security.TCredentials credentials
  )

  void cancelTask(
    1:client.TInfo tinfo
    2:security.TCredentials credentials
    3:string taskID
  )
  
}

service TaskRunner {

  TaskObject getRunningTask(
    1:client.TInfo tinfo
    2:security.TCredentials credentials
  ) throws (
    1:client.ThriftSecurityException sec
  )

  string getRunningTaskId(
    1:client.TInfo tinfo
    2:security.TCredentials credentials
  ) throws (
    1:client.ThriftSecurityException sec
  )

  void cancelTask(
    1:client.TInfo tinfo
    2:security.TCredentials credentials
    3:string taskID
  )

}
