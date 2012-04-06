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
package org.apache.accumulo.server.tabletserver.log;

import java.util.Set;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.server.conf.ServerConfiguration;

public abstract class LoggerStrategy {
  private ServerConfiguration configuration = null;
  
  public void init(ServerConfiguration configuration) {
    this.configuration = configuration;
  }

  // Called by the tablet server to get the list of loggers to use from the available set
  public abstract Set<String> getLoggers(Set<String> allLoggers);
  
  // Called by the master (via the tablet server) to prefer loggers for balancing
  public abstract void preferLoggers(Set<String> preference);
  
  public int getNumberOfLoggersToUse() {
    return configuration.getConfiguration().getCount(Property.TSERV_LOGGER_COUNT);
  }
}
