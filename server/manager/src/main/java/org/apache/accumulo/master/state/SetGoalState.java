/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.master.state;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @deprecated since 2.1.0. Use {@link org.apache.accumulo.manager.state.SetGoalState} instead.
 */
@Deprecated(since = "2.1.0")
public class SetGoalState {
  final static private Logger log = LoggerFactory.getLogger(SetGoalState.class);

  /**
   * Utility program that will change the goal state for the master from the command line.
   */
  public static void main(String[] args) throws Exception {
    log.warn("{} is deprecated. Use {} instead.", SetGoalState.class.getName(),
        org.apache.accumulo.manager.state.SetGoalState.class.getName());
    org.apache.accumulo.manager.state.SetGoalState.main(args);
  }
}
