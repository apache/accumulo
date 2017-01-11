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

/**
 * Thrown when the server specified doesn't exist when it was expected to
 */
public class TabletServerNotFoundException extends Exception {
  /**
   * Exception to throw if an operation is attempted on a namespace that doesn't exist.
   *
   */
  private static final long serialVersionUID = 1L;

  private String server;

  /**
   * @param description
   *          the specific reason why it failed
   */
  public TabletServerNotFoundException(String description) {
    super(description != null && !description.isEmpty() ? description : "");
  }

  /**
   * @param serverName
   *          the visible name of the server that was sought
   * @param description
   *          the specific reason why it failed
   */
  public TabletServerNotFoundException(String serverName, String description) {
    super("Server" + (serverName != null && !serverName.isEmpty() ? " " + serverName : "") + " does not exist"
        + (description != null && !description.isEmpty() ? description : ""));
    this.server = serverName;
  }

  /**
   * @param serverName
   *          the visible name of the server that was sought
   * @param description
   *          the specific reason why it failed
   * @param cause
   *          the exception that caused this failure
   */
  public TabletServerNotFoundException(String serverName, String description, Throwable cause) {
    this(serverName, description);
    super.initCause(cause);
  }

  /**
   * @return the name of the server sought
   */
  public String getServerName() {
    return server;
  }
}
