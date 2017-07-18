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
package org.apache.accumulo.tserver.session;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.client.Durability;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.thrift.TCredentials;

public class ConditionalSession extends Session {
  public final TCredentials credentials;
  public final Authorizations auths;
  public final Table.ID tableId;
  public final AtomicBoolean interruptFlag = new AtomicBoolean();
  public final Durability durability;
  public final String classLoaderContext;

  public ConditionalSession(TCredentials credentials, Authorizations authorizations, Table.ID tableId, Durability durability, String classLoaderContext) {
    super(credentials);
    this.credentials = credentials;
    this.auths = authorizations;
    this.tableId = tableId;
    this.durability = durability;
    this.classLoaderContext = classLoaderContext;
  }

  @Override
  public boolean cleanup() {
    interruptFlag.set(true);
    return true;
  }
}
