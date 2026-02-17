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
package org.apache.accumulo.server.util;

import org.apache.accumulo.core.cli.BaseKeywordExecutable;
import org.apache.accumulo.core.cli.ServerOpts;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.security.SecurityUtil;

import com.beust.jcommander.JCommander;

public abstract class ServerKeywordExecutable<OPTS extends ServerOpts>
    extends BaseKeywordExecutable<OPTS> {

  private final ServerContext context;

  public ServerKeywordExecutable(OPTS options) {
    super(options);
    context = new ServerContext(SiteConfiguration.auto());
  }

  public ServerContext getServerContext() {
    if (context.isClosed()) {
      throw new IllegalStateException(
          "Cannot use context after execute method has completed. Context is closed.");
    }
    return context;
  }

  @Override
  public void doExecute(JCommander cl, OPTS options) throws Exception {
    // Login as the server on secure HDFS
    try (ServerContext context = getServerContext()) {
      AccumuloConfiguration conf = context.getConfiguration();
      if (conf.getBoolean(Property.INSTANCE_RPC_SASL_ENABLED)) {
        SecurityUtil.serverLogin(conf);
      }
      execute(cl, options);
    }
  }
}
