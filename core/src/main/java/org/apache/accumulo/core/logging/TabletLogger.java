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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.util.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

// logs messages about a tablets internal state, like its location, set of files, metadata... operations that cut across multiple tablets should go elsewhere
public class TabletLogger {

  private static final Logger locLog = LoggerFactory.getLogger("accumulo.table.tablet.location");
  private static final Logger fileLog = LoggerFactory.getLogger("accumulo.table.tablet.files");
  // TODO remove....
  private static final Logger scanLog = LoggerFactory.getLogger("accumulo.table.tablet.scan");
  private static final Logger rootMetaLog =
      LoggerFactory.getLogger("accumulo.table.tablet.metadata.root");
  private static final Logger metaMetaLog =
      LoggerFactory.getLogger("accumulo.table.tablet.metadata.meta");
  private static final Logger userMetaLog =
      LoggerFactory.getLogger("accumulo.table.tablet.metadata.user");

  private static final Gson GSON = new GsonBuilder().create();

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

  public static void metadataUpdated(KeyExtent extent, Mutation mutation) {
    Logger mlog;

    if (!extent.isMeta())
      mlog = userMetaLog;
    else if (extent.isRootTablet())
      mlog = rootMetaLog;
    else
      mlog = metaMetaLog;

    if (mlog.isTraceEnabled()) {
      String json = toJson(mutation);
      mlog.trace("Metadata updated for {} : {}", extent, json);
    }
  }

  private static String toJson(Mutation mutation) {

    Map<String,String> columnValues = new TreeMap<>();

    mutation.getUpdates().forEach(cu -> {
      String vis = "";
      if (cu.getColumnVisibility().length > 0) {
        vis = ":" + new String(cu.getColumnVisibility(), UTF_8);
      }
      String key = new String(cu.getColumnFamily(), UTF_8) + ":"
          + new String(cu.getColumnQualifier(), UTF_8) + vis + " " + cu.getTimestamp();
      String value = new String(cu.getValue());

      columnValues.put(key, value);
    });

    return GSON.toJson(columnValues);
  }
}
