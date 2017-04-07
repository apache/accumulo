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
package org.apache.accumulo.monitor.rest.tservers;

import javax.xml.bind.annotation.XmlAttribute;

/**
 *
 * Generates a bad tserver information
 *
 * @since 2.0.0
 *
 */
public class BadTabletServerInformation {

  // Variable names become JSON keys
  @XmlAttribute
  public String id;

  @XmlAttribute
  public String status;

  public BadTabletServerInformation() {}

  /**
   * Stores a bad tserver
   *
   * @param id
   *          ID of the tserver
   * @param status
   *          Status of the tserver
   */
  public BadTabletServerInformation(String id, String status) {
    this.id = id;
    this.status = status;
  }
}
