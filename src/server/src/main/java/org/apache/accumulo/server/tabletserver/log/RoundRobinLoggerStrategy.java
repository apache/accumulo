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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.server.tabletserver.TabletServer;
import org.apache.log4j.Logger;

public class RoundRobinLoggerStrategy extends LoggerStrategy {
  
  private static final Logger log = Logger.getLogger(RoundRobinLoggerStrategy.class);
  final List<String> preferences = new ArrayList<String>();
  final String myHostName;
  
  public RoundRobinLoggerStrategy(TabletServer tserver) {
    String address = tserver.getClientAddressString();
    myHostName = address.split(":", 2)[0];
  }
  
  @Override
  public Set<String> getLoggers(Set<String> allLoggers) {
    if (allLoggers.size() == 0)
      return allLoggers;
    int numberOfLoggersToUse = getNumberOfLoggersToUse();
    Set<String> result = new HashSet<String>();
    
    // use the preferred loggers if they exist
    if (!preferences.isEmpty()) {
      for (int i = 0; result.size() < numberOfLoggersToUse && i < preferences.size(); i++) {
        String preferred = preferences.get(i);
        if (allLoggers.contains(preferred))
          result.add(preferred);
      }
    }
    
    // use logs closest to us (in name)
    List<String> loggers = new ArrayList<String>(allLoggers);
    Collections.sort(loggers);
    int pos = Collections.binarySearch(loggers, myHostName);
    if (pos < 0)
      pos = -pos - 1;
    for (int i = 0; result.size() < numberOfLoggersToUse && i < loggers.size(); i++) {
      String selection = loggers.get((pos + i) % loggers.size());
      log.debug("Choosing logger " + selection);
      result.add(selection);
    }
    return result;
  }
  
  @Override
  public void preferLoggers(Set<String> preference) {
    preferences.addAll(preference);
  }
  
}
