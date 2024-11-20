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
package org.apache.accumulo.server.rest.request;

import org.apache.accumulo.core.clientImpl.thrift.TInfo;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;

public class GetCompactionJobRequest {

  private TInfo tinfo;
  private TCredentials credentials;
  private String groupName;
  private String compactorAddress;
  private String externalCompactionId;

  public GetCompactionJobRequest() {}

  public GetCompactionJobRequest(TInfo tinfo, TCredentials credentials, String groupName,
      String compactorAddress, String externalCompactionId) {
    this.tinfo = tinfo;
    this.credentials = credentials;
    this.groupName = groupName;
    this.compactorAddress = compactorAddress;
    this.externalCompactionId = externalCompactionId;
  }

  public TInfo getTinfo() {
    return tinfo;
  }

  public void setTinfo(TInfo tinfo) {
    this.tinfo = tinfo;
  }

  public TCredentials getCredentials() {
    return credentials;
  }

  public void setCredentials(TCredentials credentials) {
    this.credentials = credentials;
  }

  public String getGroupName() {
    return groupName;
  }

  public void setGroupName(String groupName) {
    this.groupName = groupName;
  }

  public String getCompactorAddress() {
    return compactorAddress;
  }

  public void setCompactorAddress(String compactorAddress) {
    this.compactorAddress = compactorAddress;
  }

  public String getExternalCompactionId() {
    return externalCompactionId;
  }

  public void setExternalCompactionId(String externalCompactionId) {
    this.externalCompactionId = externalCompactionId;
  }
}
