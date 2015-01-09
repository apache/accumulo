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
package org.apache.accumulo.trace.instrument.receivers;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.log4j.Level;

/**
 * A SpanReceiver that just logs the data using log4j.
 */
public class LogSpans implements SpanReceiver {
  private static final org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(LogSpans.class);

  static public class SpanLevel extends Level {

    private static final long serialVersionUID = 1L;

    protected SpanLevel() {
      super(Level.DEBUG_INT + 150, "SPAN", Level.DEBUG_INT + 150);
    }

    static public Level toLevel(int val) {
      if (val == Level.DEBUG_INT + 150)
        return Level.DEBUG;
      return Level.toLevel(val);
    }
  }

  public final static Level SPAN = new SpanLevel();

  public static String format(long traceId, long spanId, long parentId, long start, long stop, String description, Map<String,String> data) {
    String parentStr = "";
    if (parentId > 0)
      parentStr = " parent:" + parentId;
    String startStr = new SimpleDateFormat("HH:mm:ss.SSS").format(new Date(start));
    return String.format("%20s:%x id:%d%s start:%s ms:%d", description, traceId, spanId, parentStr, startStr, stop - start);
  }

  @Override
  public void span(long traceId, long spanId, long parentId, long start, long stop, String description, Map<String,String> data) {
    log.log(SPAN, format(traceId, spanId, parentId, start, stop, description, data));
  }

  @Override
  public void flush() {}
}
