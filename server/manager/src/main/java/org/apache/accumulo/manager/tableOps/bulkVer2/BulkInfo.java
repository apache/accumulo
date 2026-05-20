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
package org.apache.accumulo.manager.tableOps.bulkVer2;

import java.io.Serializable;
import java.lang.reflect.Type;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.manager.tableOps.bulkVer2.BulkInfo.BulkInfoSerializer;
import org.apache.hadoop.io.Text;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.gson.annotations.JsonAdapter;

/**
 * Package private class to hold all the information used for bulk import2
 */
@JsonAdapter(BulkInfoSerializer.class)
class BulkInfo implements Serializable {

  public static class BulkInfoSerializer implements JsonSerializer<BulkInfo> {

    @Override
    public JsonElement serialize(BulkInfo src, Type typeOfSrc, JsonSerializationContext context) {
      JsonObject obj = new JsonObject();
      obj.addProperty("tableId", src.tableId.canonical());
      obj.addProperty("sourceDir", src.sourceDir);
      obj.addProperty("bulkDir", src.bulkDir);
      obj.addProperty("setTime", src.setTime);
      if (src.firstSplit != null) {
        obj.addProperty("firstSplit", new Text(src.firstSplit).toString());
      }
      if (src.lastSplit != null) {
        obj.addProperty("lastSplit", new Text(src.lastSplit).toString());
      }
      return obj;
    }

  }

  private static final long serialVersionUID = 1L;

  TableId tableId;
  String sourceDir;
  String bulkDir;
  boolean setTime;
  // firstSplit and lastSplit describe the min and max splits in the table that overlap the bulk
  // imported data
  byte[] firstSplit;
  byte[] lastSplit;

  static BulkInfo create(TableId tableId, String sourceDir, boolean setTime) {
    BulkInfo info = new BulkInfo();
    info.tableId = tableId;
    info.sourceDir = sourceDir;
    info.setTime = setTime;
    return info;
  }
}
