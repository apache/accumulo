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

import org.apache.accumulo.core.client.impl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.constraints.Constraint.Environment;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.security.AuthorizationContainer;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.thrift.TCredentials;
import org.apache.accumulo.server.security.SecurityOperation;

public class TservConstraintEnv implements Environment {

  private final TCredentials credentials;
  private final SecurityOperation security;
  private Authorizations auths;
  private KeyExtent ke;

  TservConstraintEnv(SecurityOperation secOp, TCredentials credentials) {
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
  @Deprecated
  public Authorizations getAuthorizations() {
    if (auths == null)
      try {
        this.auths = security.getUserAuthorizations(credentials);
      } catch (ThriftSecurityException e) {
        throw new RuntimeException(e);
      }
    return auths;
  }

  @Override
  public AuthorizationContainer getAuthorizationsContainer() {
    return new AuthorizationContainer() {
      @Override
      public boolean contains(ByteSequence auth) {
        try {
          return security.userHasAuthorizations(credentials,
              Collections.<ByteBuffer> singletonList(ByteBuffer.wrap(auth.getBackingArray(), auth.offset(), auth.length())));
        } catch (ThriftSecurityException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }
}
