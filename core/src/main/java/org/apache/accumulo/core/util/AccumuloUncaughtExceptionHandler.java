/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.util;

import java.lang.Thread.UncaughtExceptionHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AccumuloUncaughtExceptionHandler implements UncaughtExceptionHandler {

  private static final Logger log = LoggerFactory.getLogger(AccumuloUncaughtExceptionHandler.class);
  private static final String HALT_PROPERTY = "HaltVMOnThreadError";

  @Override
  public void uncaughtException(Thread t, Throwable e) {
    if (e instanceof Exception) {
      log.error(String.format("Caught an exception in %s. Thread is dead.", t), e);
    } else {
      if (System.getProperty(HALT_PROPERTY, "false").equals("true")) {
        log.error(String.format("Caught an exception in %s.", t), e);
        Halt.halt(String.format("Caught an exception in %s. Halting VM, check the logs.", t));
      } else {
        log.error(String.format("Caught an exception in %s. Thread is dead.", t), e);
      }
    }
  }

}
