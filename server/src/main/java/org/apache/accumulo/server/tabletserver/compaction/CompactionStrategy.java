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
package org.apache.accumulo.server.tabletserver.compaction;

import java.io.IOException;

/**
 * The interface for customizing major compactions.
 */
public abstract class CompactionStrategy {
  
  /**
   * Called prior to obtaining the tablet lock, useful for examining metadata or indexes.
   * @param request basic details about the tablet
   * @throws IOException
   */
  public void gatherInformation(MajorCompactionRequest request) throws IOException {
    
  }
  
  /** 
   * Get the plan for compacting a tablets files.  Called while holding the tablet lock, so it should not be doing any blocking.
   * @param request basic details about the tablet
   * @return the plan for a major compaction
   * @throws IOException
   */
  abstract public CompactionPlan getCompactionPlan(MajorCompactionRequest request) throws IOException;
  
}
