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
package org.apache.accumulo.core.client.impl;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.thrift.TApplicationException;

/**
 * This class is intended to encapsulate errors that occurred on the server side.
 *
 */

public class AccumuloServerException extends AccumuloException {
  private static final long serialVersionUID = 1L;
  private String server;

  AccumuloServerException(final AccumuloServerException cause) {
    super("Error on server " + cause.getServer(), cause);
  }

  public AccumuloServerException(final String server, final TApplicationException tae) {
    super("Error on server " + server, tae);
    this.setServer(server);
  }

  private void setServer(final String server) {
    this.server = server;
  }

  public String getServer() {
    return server;
  }

}
