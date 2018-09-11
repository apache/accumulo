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
package org.apache.accumulo.core;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.impl.AccumuloClientImpl;

/**
 * The main entry point for Accumulo public API.
 *
 * @since 2.0.0
 */
public final class Accumulo {

  private Accumulo() {}

  /**
   * Create an Accumulo client builder, used to construct a client. For example:
   *
   * {@code Accumulo.newClient().forInstance(instanceName, zookeepers)
   *         .usingPassword(user, password).withZkTimeout(1234).build();}
   *
   * @return a builder object for Accumulo clients
   */
  public static AccumuloClient.ClientInfoOptions newClient() {
    return new AccumuloClientImpl.AccumuloClientBuilderImpl();
  }

}
