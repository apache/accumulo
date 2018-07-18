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
package org.apache.accumulo.server.cli;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.server.client.HdfsZooInstance;

public class ClientOpts extends org.apache.accumulo.core.cli.ClientOpts {
  {
    setPrincipal("root");
  }

  public ClientContext getClientContext() {
    return new ClientContext(getClientInfo());
  }

  @Override
  public Instance getInstance() {
    if (instance == null) {
      return HdfsZooInstance.getInstance();
    }
    try {
      return getConnector().getInstance();
    } catch (AccumuloSecurityException | AccumuloException e) {
      throw new IllegalStateException(e);
    }
  }
}
