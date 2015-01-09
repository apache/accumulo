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

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.server.client.HdfsZooInstance;

public class ClientOnDefaultTable extends org.apache.accumulo.core.cli.ClientOnDefaultTable {
  {
    principal = "root";
  }

  @Override
  synchronized public Instance getInstance() {
    if (cachedInstance != null)
      return cachedInstance;

    if (mock)
      return cachedInstance = new MockInstance(instance);
    if (instance == null) {
      return cachedInstance = HdfsZooInstance.getInstance();
    }
    return cachedInstance = new ZooKeeperInstance(this.getClientConfiguration());
  }

  public ClientOnDefaultTable(String table) {
    super(table);
  }
}
