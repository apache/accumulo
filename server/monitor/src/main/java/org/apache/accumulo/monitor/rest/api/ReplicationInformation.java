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
package org.apache.accumulo.monitor.rest.api;

public class ReplicationInformation {

  public String tableName, peerName, remoteIdentifier, replicaSystemType;
  public long filesNeedingReplication;

  public ReplicationInformation() {}

  public ReplicationInformation(String tableName, String peerName, String remoteIdentifier, String replicaSystemType, long filesNeedingReplication) {
    this.tableName = tableName;
    this.peerName = peerName;
    this.remoteIdentifier = remoteIdentifier;
    this.replicaSystemType = replicaSystemType;
    this.filesNeedingReplication = filesNeedingReplication;
  }
}
