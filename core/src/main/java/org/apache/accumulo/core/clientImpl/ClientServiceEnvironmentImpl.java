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
package org.apache.accumulo.core.clientImpl;

import java.io.IOException;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.accumulo.core.util.ConfigurationImpl;

public class ClientServiceEnvironmentImpl implements ServiceEnvironment {

  private final ClientContext context;

  public ClientServiceEnvironmentImpl(ClientContext context) {
    this.context = context;
  }

  @Override
  public Configuration getConfiguration() {
    try {
      return new ConfigurationImpl(
          new ConfigurationCopy(context.instanceOperations().getSystemConfiguration()));
    } catch (AccumuloException | AccumuloSecurityException e) {
      throw new RuntimeException("Error getting system configuration", e);
    }
  }

  @Override
  public Configuration getConfiguration(TableId tableId) {
    try {
      return new ConfigurationImpl(
          new ConfigurationCopy(context.tableOperations().getConfiguration(getTableName(tableId))));
    } catch (AccumuloException | TableNotFoundException e) {
      throw new RuntimeException("Error getting table configuration", e);
    }
  }

  @Override
  public String getTableName(TableId tableId) throws TableNotFoundException {
    return context.getTableName(tableId);
  }

  @Override
  public <T> T instantiate(String className, Class<T> base)
      throws ReflectiveOperationException, IOException {
    return ClientServiceEnvironmentImpl.class.getClassLoader().loadClass(className).asSubclass(base)
        .getDeclaredConstructor().newInstance();
  }

  @Override
  public <T> T instantiate(TableId tableId, String className, Class<T> base)
      throws ReflectiveOperationException, IOException {
    return instantiate(className, base);
  }
}
