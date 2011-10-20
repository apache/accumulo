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

import static java.lang.Math.min;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.server.tabletserver.TabletServer;

public class RandomLoggerStrategy extends LoggerStrategy {
  
  public RandomLoggerStrategy() {}
  
  public RandomLoggerStrategy(TabletServer tserver) {}
  
  @Override
  public Set<String> getLoggers(Set<String> allLoggers) {
    List<String> copy = new ArrayList<String>(allLoggers);
    Collections.shuffle(copy);
    return new HashSet<String>(copy.subList(0, min(copy.size(), getNumberOfLoggersToUse())));
  }
  
  @Override
  public void preferLoggers(Set<String> preference) {
    // ignored
  }
  
}
