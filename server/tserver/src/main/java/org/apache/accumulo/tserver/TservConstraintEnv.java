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
package org.apache.accumulo.tserver;

import java.nio.ByteBuffer;
import java.util.Collections;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.security.AuthorizationContainer;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.constraints.SystemEnvironment;
import org.apache.accumulo.server.security.SecurityOperation;

public class TservConstraintEnv implements SystemEnvironment {

  private final ServerContext context;
  private final TCredentials credentials;
  private final SecurityOperation security;
  private KeyExtent ke;

  TservConstraintEnv(ServerContext context, SecurityOperation secOp, TCredentials credentials) {
    this.context = context;
    this.security = secOp;
    this.credentials = credentials;
  }

  public void setExtent(KeyExtent ke) {
    this.ke = ke;
  }

  @Override
  public KeyExtent getExtent() {
    return ke;
  }

  @Override
  public String getUser() {
    return credentials.getPrincipal();
  }

  @Override
  public AuthorizationContainer getAuthorizationsContainer() {
    return new AuthorizationContainer() {
      @Override
      public boolean contains(ByteSequence auth) {
        return security.authenticatedUserHasAuthorizations(credentials, Collections
            .singletonList(ByteBuffer.wrap(auth.getBackingArray(), auth.offset(), auth.length())));
      }
    };
  }

  @Override
  public ServerContext getServerContext() {
    return context;
  }
}
