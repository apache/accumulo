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
package org.apache.accumulo.core.metadata.schema;

import java.util.Base64;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.hadoop.io.Text;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;

public class ExternalCompactionFinalState {

  private static final Gson GSON = new Gson();

  public enum FinalState {
    FINISHED, FAILED
  }

  private final ExternalCompactionId ecid;
  private final KeyExtent extent;
  private final FinalState state;
  private final long fileSize;
  private final long fileEntries;

  public ExternalCompactionFinalState(ExternalCompactionId ecid, KeyExtent extent, FinalState state,
      long fileSize, long fileEntries) {
    this.ecid = ecid;
    this.extent = extent;
    this.state = state;
    this.fileSize = fileSize;
    this.fileEntries = fileEntries;
  }

  public ExternalCompactionId getExternalCompactionId() {
    return ecid;
  }

  public FinalState getFinalState() {
    return state;
  }

  public KeyExtent getExtent() {
    return extent;
  }

  public long getFileSize() {
    Preconditions.checkState(state == FinalState.FINISHED);
    return fileSize;
  }

  public long getEntries() {
    Preconditions.checkState(state == FinalState.FINISHED);
    return fileEntries;
  }

  // This class is used to serialize and deserialize this class using GSon. Any changes to this
  // class must consider persisted data.
  private static class Extent {

    final String tableId;
    final String er;
    final String per;

    Extent(KeyExtent extent) {
      this.tableId = extent.tableId().canonical();
      if (extent.endRow() != null) {
        er = Base64.getEncoder().encodeToString(TextUtil.getBytes(extent.endRow()));
      } else {
        er = null;
      }

      if (extent.prevEndRow() != null) {
        per = Base64.getEncoder().encodeToString(TextUtil.getBytes(extent.prevEndRow()));
      } else {
        per = null;
      }
    }

    private Text decode(String s) {
      if (s == null) {
        return null;
      }
      return new Text(Base64.getDecoder().decode(s));
    }

    KeyExtent toKeyExtent() {
      return new KeyExtent(TableId.of(tableId), decode(er), decode(per));
    }
  }

  // This class is used to serialize and deserialize this class using GSon. Any changes to this
  // class must consider persisted data.
  private static class JsonData {
    Extent extent;
    String state;
    long fileSize;
    long entries;
  }

  public String toJson() {
    JsonData jd = new JsonData();
    jd.state = state.name();
    jd.fileSize = fileSize;
    jd.entries = fileEntries;
    jd.extent = new Extent(extent);
    return GSON.toJson(jd);
  }

  public static ExternalCompactionFinalState fromJson(ExternalCompactionId ecid, String json) {
    JsonData jd = GSON.fromJson(json, JsonData.class);
    return new ExternalCompactionFinalState(ecid, jd.extent.toKeyExtent(),
        FinalState.valueOf(jd.state), jd.fileSize, jd.entries);
  }

  @Override
  public String toString() {
    return toJson();
  }
}
