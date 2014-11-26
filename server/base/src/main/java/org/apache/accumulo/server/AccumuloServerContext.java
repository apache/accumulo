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
package org.apache.accumulo.server;

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.core.util.SslConnectionParams;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.accumulo.server.security.SystemCredentials;

/**
 * Provides a server context for Accumulo server components that operate with the system credentials and have access to the system files and configuration.
 */
public class AccumuloServerContext extends ClientContext {

  private final ServerConfigurationFactory confFactory;

  /**
   * Construct a server context from the server's configuration
   */
  public AccumuloServerContext(ServerConfigurationFactory confFactory) {
    super(confFactory.getInstance(), getCredentials(confFactory.getInstance()), confFactory.getConfiguration());
    this.confFactory = confFactory;
  }

  /**
   * Get the credentials to use for this instance so it can be passed to the superclass during construction.
   */
  private static Credentials getCredentials(Instance instance) {
    if (instance instanceof MockInstance) {
      return new Credentials("mockSystemUser", new PasswordToken("mockSystemPassword"));
    }
    return SystemCredentials.get(instance);
  }

  /**
   * Retrieve the configuration factory used to construct this context
   */
  public ServerConfigurationFactory getServerConfigurationFactory() {
    return confFactory;
  }

  /**
   * Retrieve the SSL/TLS configuration for starting up a listening service
   */
  public SslConnectionParams getServerSslParams() {
    return SslConnectionParams.forServer(getConfiguration());
  }

}
