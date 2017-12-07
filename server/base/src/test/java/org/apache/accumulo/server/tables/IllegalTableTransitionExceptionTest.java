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
package org.apache.accumulo.server.tables;

import static org.junit.Assert.assertEquals;

import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.server.tables.TableManager.IllegalTableTransitionException;
import org.junit.Test;

public class IllegalTableTransitionExceptionTest {

  final TableState oldState = TableState.ONLINE;
  final TableState newState = TableState.OFFLINE;
  final String defaultMsg = "Error transitioning from " + oldState + " state to " + newState + " state";

  @Test
  public void testIllegalTableTransitionExceptionMessage() {
    String userMessage = null;
    try {
      userMessage = "User suppled message - Exception from " + oldState + " state to " + newState + " state";
      throw new TableManager.IllegalTableTransitionException(oldState, newState, userMessage);
    } catch (IllegalTableTransitionException e) {
      assertEquals(userMessage, e.getMessage());
    }
  }

  @Test
  public void testIllegalTableTransitionExceptionDefaultMessage() {
    try {
      throw new TableManager.IllegalTableTransitionException(oldState, newState);
    } catch (IllegalTableTransitionException e) {
      assertEquals(defaultMsg, e.getMessage());
    }
  }

  @Test
  public void testIllegalTableTransitionExceptionWithNull() {
    try {
      throw new TableManager.IllegalTableTransitionException(oldState, newState, null);
    } catch (IllegalTableTransitionException e) {
      assertEquals(defaultMsg, e.getMessage());
    }
  }

  @Test
  public void testIllegalTableTransitionExceptionEmptyMessage() {
    try {
      throw new TableManager.IllegalTableTransitionException(oldState, newState, "");
    } catch (IllegalTableTransitionException e) {
      assertEquals(defaultMsg, e.getMessage());
    }
  }
}
