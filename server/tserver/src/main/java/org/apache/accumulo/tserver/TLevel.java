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
package org.apache.accumulo.tserver;

import org.apache.log4j.Level;
import org.apache.log4j.Priority;
import org.slf4j.Logger;

public class TLevel extends Level {

  private static final long serialVersionUID = 1L;
  public final static Level TABLET_HIST = new TLevel();

  protected TLevel() {
    super(Priority.DEBUG_INT + 100, "TABLET_HIST", Priority.DEBUG_INT + 100);
  }


  static public void logAtLevel(Logger log, Level level, String msg, Object...objects) {
    switch(level.toInt()) {
      case Priority.DEBUG_INT:
        log.debug(msg, objects);
        break;
      case Priority.ERROR_INT:
      case Priority.FATAL_INT:
        log.error(msg, objects);
        break;
      case Priority.INFO_INT:
        log.info(msg, objects);
        break;
      case Level.TRACE_INT:
        log.trace(msg, objects);
        break;
      case Priority.WARN_INT:
        log.warn(msg, objects);
        break;
    }
  }
}
