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

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.cli.ServerUtilOpts;
import org.apache.accumulo.server.security.SecurityUtil;
import org.apache.accumulo.start.spi.KeywordExecutable;

import com.beust.jcommander.JCommander;

public abstract class ServerKeywordExecutable<O extends ServerUtilOpts>
    implements KeywordExecutable {

  private final O options;

  public ServerKeywordExecutable(O options) {
    this.options = options;
  }

  @Override
  public final void execute(String[] args) throws Exception {
    JCommander cl = new JCommander(this.options);
    cl.setProgramName("accumulo " + usageGroup().name().toLowerCase() + " " + keyword());
    cl.parse(args);

    if (this.options.help) {
      cl.usage();
      return;
    }
    // Login as the server on secure HDFS
    try (ServerContext context = options.getServerContext()) {
      AccumuloConfiguration conf = context.getConfiguration();
      if (conf.getBoolean(Property.INSTANCE_RPC_SASL_ENABLED)) {
        SecurityUtil.serverLogin(conf);
      }
      execute(cl, options);
    }
  }

  public abstract void execute(JCommander cl, O options) throws Exception;
}
