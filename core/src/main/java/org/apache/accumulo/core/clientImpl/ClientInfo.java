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

import java.net.URL;
import java.nio.file.Path;
import java.util.Properties;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.hadoop.conf.Configuration;

/**
 * Accumulo client information. Can be built using {@link Accumulo#newClient()}
 *
 * @since 2.0.0
 */
public interface ClientInfo {

  /**
   * @return Accumulo instance name
   */
  String getInstanceName();

  /**
   * @return Zookeeper connection information for Accumulo instance
   */
  String getZooKeepers();

  /**
   * @return ZooKeeper connection timeout
   */
  int getZooKeepersSessionTimeOut();

  /**
   * @return Accumulo principal/username
   */
  String getPrincipal();

  /**
   * @return {@link AuthenticationToken} used for this connection
   */
  AuthenticationToken getAuthenticationToken();

  /**
   * @return True if SASL enabled
   */
  boolean saslEnabled();

  /**
   * @return All Accumulo client properties set for this connection
   */
  Properties getProperties();

  /**
   * @return hadoop Configuration
   */
  Configuration getHadoopConf();

  /**
   * @return ClientInfo given properties
   */
  static ClientInfo from(Properties properties) {
    return new ClientInfoImpl(properties);
  }

  /**
   * @return ClientInfo given URL path to client config file
   */
  static ClientInfo from(URL propertiesURL) {
    return new ClientInfoImpl(propertiesURL);
  }

  /**
   * @return ClientInfo given properties and token
   */
  static ClientInfo from(Properties properties, AuthenticationToken token) {
    return new ClientInfoImpl(properties, token);
  }

  /**
   * @return ClientInfo given path to client config file
   */
  static ClientInfo from(Path propertiesFile) {
    return new ClientInfoImpl(propertiesFile);
  }
}
