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
package org.apache.accumulo.tserver.metrics;

public interface TabletServerMBean {
  
  public int getOnlineCount();
  
  public int getOpeningCount();
  
  public int getUnopenedCount();
  
  public int getMajorCompactions();
  
  public int getMajorCompactionsQueued();
  
  public int getMinorCompactions();
  
  public int getMinorCompactionsQueued();
  
  public long getEntries();
  
  public long getEntriesInMemory();
  
  public long getQueries();
  
  public long getIngest();
  
  public long getTotalMinorCompactions();
  
  public double getHoldTime();
  
  public String getName();
  
  public double getAverageFilesPerTablet();
}
