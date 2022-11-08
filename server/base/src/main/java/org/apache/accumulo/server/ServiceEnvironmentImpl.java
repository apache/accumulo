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
package org.apache.accumulo.server;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.accumulo.core.classloader.ClassLoaderUtil;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.accumulo.core.util.ConfigurationImpl;

public class ServiceEnvironmentImpl implements ServiceEnvironment {

  private final ServerContext context;
  private final Configuration conf;
  private final Map<TableId,Configuration> tableConfigs = new ConcurrentHashMap<>();

  public ServiceEnvironmentImpl(ServerContext context) {
    this.context = context;
    this.conf = new ConfigurationImpl(this.context.getConfiguration());
  }

  @Override
  public Configuration getConfiguration() {
    return conf;
  }

  @Override
  public Configuration getConfiguration(TableId tableId) {
    return tableConfigs.computeIfAbsent(tableId,
        tid -> new ConfigurationImpl(context.getTableConfiguration(tid)));
  }

  @Override
  public String getTableName(TableId tableId) throws TableNotFoundException {
    return context.getTableName(tableId);
  }

  @Override
  public <T> T instantiate(String className, Class<T> base)
      throws ReflectiveOperationException, IOException {
    return ConfigurationTypeHelper.getClassInstance(null, className, base);
  }

  @Override
  public <T> T instantiate(TableId tableId, String className, Class<T> base)
      throws ReflectiveOperationException, IOException {
    String ctx = ClassLoaderUtil.tableContext(context.getTableConfiguration(tableId));
    return ConfigurationTypeHelper.getClassInstance(ctx, className, base);
  }

  public ServerContext getContext() {
    return context;
  }
}
