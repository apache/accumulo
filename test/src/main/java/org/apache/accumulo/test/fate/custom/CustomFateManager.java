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
package org.apache.accumulo.test.fate.custom;

import java.io.IOException;
import java.util.function.BiFunction;

import org.apache.accumulo.core.cli.ServerOpts;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.core.fate.Fate;
import org.apache.accumulo.core.fate.FateStore;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.FateEnv;
import org.apache.accumulo.server.ServerContext;

abstract class CustomFateManager extends Manager {
  protected CustomFateManager(ServerOpts opts,
      BiFunction<SiteConfiguration,ResourceGroupId,ServerContext> serverContextFactory,
      String[] args) throws IOException {
    super(opts, serverContextFactory, args);
  }

  @Override
  protected abstract Fate<FateEnv> createFateInstance(FateEnv env, FateStore<FateEnv> store,
      ServerContext context);
}
