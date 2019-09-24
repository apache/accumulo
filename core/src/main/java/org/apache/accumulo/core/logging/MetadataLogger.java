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

import java.util.Map;
import java.util.TreeMap;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

// Log updates to Accumulo's metadata
public class MetadataLogger {

  private static final Gson GSON = new GsonBuilder().create();

  private static final Logger rootTabletLog =
      LoggerFactory.getLogger("accumulo.metadata.root.tablet");
  private static final Logger metaTabletsLog =
      LoggerFactory.getLogger("accumulo.metadata.meta.tablet");
  private static final Logger userTabletsLog =
      LoggerFactory.getLogger("accumulo.metadata.user.tablet");

  // TODO logging for blips and del markers

  private static final Logger rootDelLog = LoggerFactory.getLogger("accumulo.metadata.root.del");
  private static final Logger metaDelLog = LoggerFactory.getLogger("accumulo.metadata.meta.del");
  private static final Logger userDelLog = LoggerFactory.getLogger("accumulo.metadata.user.del");

  public static void tabletMutated(KeyExtent extent, Mutation mutation) {
    Logger mlog;

    if (!extent.isMeta())
      mlog = userTabletsLog;
    else if (extent.isRootTablet())
      mlog = rootTabletLog;
    else
      mlog = metaTabletsLog;

    if (mlog.isTraceEnabled()) {
      String json = toJson(mutation);
      mlog.trace("Tablet {} mutated {}", extent, json);
    }
  }

  private static String toJson(Mutation mutation) {

    // TODO is there something better for logging a mutation???

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
