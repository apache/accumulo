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

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 *
 */
public class ReplicationInformation {

  private String tableName, peerName, remoteIdentifier, replicaSystemType;
  private long filesNeedingReplication;

  public ReplicationInformation() {}

  public ReplicationInformation(String tableName, String peerName, String remoteIdentifier, String replicaSystemType, long filesNeedingReplication) {
    this.tableName = tableName;
    this.peerName = peerName;
    this.remoteIdentifier = remoteIdentifier;
    this.replicaSystemType = replicaSystemType;
    this.filesNeedingReplication = filesNeedingReplication;
  }

  @JsonProperty("tableName")
  public String getTableName() {
    return tableName;
  }

  @JsonProperty("tableName")
  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  @JsonProperty("peerName")
  public String getPeerName() {
    return peerName;
  }

  @JsonProperty("peerName")
  public void setPeerName(String peerName) {
    this.peerName = peerName;
  }

  @JsonProperty("remoteIdentifier")
  public String getRemoteIdentifier() {
    return remoteIdentifier;
  }

  @JsonProperty("remoteIdentifier")
  public void setRemoteIdentifier(String remoteIdentifier) {
    this.remoteIdentifier = remoteIdentifier;
  }

  @JsonProperty("replicaSystemType")
  public String getReplicaSystemType() {
    return replicaSystemType;
  }

  @JsonProperty("replicaSystemType")
  public void setReplicaSystemType(String replicaSystemType) {
    this.replicaSystemType = replicaSystemType;
  }

  @JsonProperty("filesNeedingReplication")
  public long getFilesNeedingReplication() {
    return filesNeedingReplication;
  }

  @JsonProperty("filesNeedingReplication")
  public void setFilesNeedingReplication(long filesNeedingReplication) {
    this.filesNeedingReplication = filesNeedingReplication;
  }
}
