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
package org.apache.accumulo.monitor.rest.zk;

/**
 *
 * Generates a new zookeeper information as a JSON object
 *
 * @since 2.0.0
 *
 */
public class ZooKeeper {

  // Variable names become JSON keys
  public String server;
  public String mode;

  public Integer clients;

  public ZooKeeper() {}

  /**
   * Stores Zookeeper information
   *
   * @param server
   *          Location of the ZK
   * @param mode
   *          ZK mode
   * @param clients
   *          Number of clients per ZK
   */
  public ZooKeeper(String server, String mode, Integer clients) {
    this.server = server;
    this.mode = mode;
    this.clients = clients;
  }
}
