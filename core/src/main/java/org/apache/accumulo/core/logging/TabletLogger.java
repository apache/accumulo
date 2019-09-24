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
package org.apache.accumulo.core.logging;

import java.util.Collection;

import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.util.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// logs messages about a tablets internal state, like its location, set of files, metadata... operations that cut across multiple tablets should go elsewhere
public class TabletLogger {

  private static final Logger locLog = LoggerFactory.getLogger("accumulo.table.tablet.location");
  private static final Logger fileLog = LoggerFactory.getLogger("accumulo.table.tablet.files");
  // TODO remove....
  private static final Logger scanLog = LoggerFactory.getLogger("accumulo.table.tablet.scan");

  public static void assigned(KeyExtent extent, Ample.TServer server) {
    locLog.debug("Assigned {} to {}", extent, server);
  }

  public static void loading(KeyExtent extent, Ample.TServer server) {
    locLog.trace("Loading {} on {}", extent, server);
  }

  public static void loaded(KeyExtent extent, Ample.TServer server) {
    locLog.debug("Loaded {} on {}", extent, server);
  }

  public static void unloaded(KeyExtent extent, Ample.TServer server) {
    locLog.debug("Unloaded {} from {}", extent, server);
  }

  public static void compacted(KeyExtent extent, Collection<? extends Ample.FileMeta> inputs,
      Ample.FileMeta output, long read, long written, long outputSize, long duration) {
    fileLog.debug("Compacted {} read:{} wrote:{} time:{}ms file:{} {} bytes inputs:{}", extent,
        read, written, duration, output, outputSize, inputs);
  }

  public enum FileSource {
    MINC, BULK
  }

  public static void addedFile(KeyExtent extent, Ample.FileMeta file, FileSource source) {
    fileLog.debug("New file for {} from {} : {}  ", extent, source, file);
  }

  public static void scanned(KeyExtent extent, String client, long entries, long duration,
      Stat stats) {
    // TODO move this to ScanLogger

    if (scanLog.isTraceEnabled()) {
      scanLog.trace(String.format("Scanned {} from {}  returned {} entries in {}ms nbTimes=[{}] ",
          extent, client, entries, duration, stats));
    }
  }

}
