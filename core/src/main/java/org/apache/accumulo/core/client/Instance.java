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
package org.apache.accumulo.core.client;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;

/**
 * This class represents the information a client needs to know to connect to an instance of accumulo.
 *
 */
public interface Instance {
  /**
   * Returns the location of the tablet server that is serving the root tablet.
   *
   * @return location in "hostname:port" form
   */
  String getRootTabletLocation();

  /**
   * Returns the location(s) of the accumulo master and any redundant servers.
   *
   * @return a list of locations in "hostname:port" form
   */
  List<String> getMasterLocations();

  /**
   * Returns a unique string that identifies this instance of accumulo.
   *
   * @return a UUID
   */
  String getInstanceID();

  /**
   * Returns the instance name given at system initialization time.
   *
   * @return current instance name
   */
  String getInstanceName();

  /**
   * Returns a comma-separated list of zookeeper servers the instance is using.
   *
   * @return the zookeeper servers this instance is using in "hostname:port" form
   */
  String getZooKeepers();

  /**
   * Returns the zookeeper connection timeout.
   *
   * @return the configured timeout to connect to zookeeper
   */
  int getZooKeepersSessionTimeOut();

  /**
   * Returns a connection to accumulo.
   *
   * @param user
   *          a valid accumulo user
   * @param pass
   *          A UTF-8 encoded password. The password may be cleared after making this call.
   * @return the accumulo Connector
   * @throws AccumuloException
   *           when a generic exception occurs
   * @throws AccumuloSecurityException
   *           when a user's credentials are invalid
   * @deprecated since 1.5, use {@link #getConnector(String, AuthenticationToken)} with {@link PasswordToken}
   */
  @Deprecated
  Connector getConnector(String user, byte[] pass) throws AccumuloException, AccumuloSecurityException;

  /**
   * Returns a connection to accumulo.
   *
   * @param user
   *          a valid accumulo user
   * @param pass
   *          A UTF-8 encoded password. The password may be cleared after making this call.
   * @return the accumulo Connector
   * @throws AccumuloException
   *           when a generic exception occurs
   * @throws AccumuloSecurityException
   *           when a user's credentials are invalid
   * @deprecated since 1.5, use {@link #getConnector(String, AuthenticationToken)} with {@link PasswordToken}
   */
  @Deprecated
  Connector getConnector(String user, ByteBuffer pass) throws AccumuloException, AccumuloSecurityException;

  /**
   * Returns a connection to this instance of accumulo.
   *
   * @param user
   *          a valid accumulo user
   * @param pass
   *          If a mutable CharSequence is passed in, it may be cleared after this call.
   * @return the accumulo Connector
   * @throws AccumuloException
   *           when a generic exception occurs
   * @throws AccumuloSecurityException
   *           when a user's credentials are invalid
   * @deprecated since 1.5, use {@link #getConnector(String, AuthenticationToken)} with {@link PasswordToken}
   */
  @Deprecated
  Connector getConnector(String user, CharSequence pass) throws AccumuloException, AccumuloSecurityException;

  /**
   * Returns a connection to this instance of accumulo.
   *
   * @param principal
   *          a valid accumulo user
   * @param token
   *          Use the token type configured for the Accumulo instance you are connecting to. An Accumulo instance with default configurations will use
   *          {@link PasswordToken}
   * @since 1.5.0
   */
  Connector getConnector(String principal, AuthenticationToken token) throws AccumuloException, AccumuloSecurityException;
}
