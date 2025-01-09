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
package org.apache.accumulo.core.metadata;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;

import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.StoredTabletFile.TabletFileCqMetadataGson;
import org.apache.accumulo.core.util.json.ByteArrayToBase64TypeAdapter;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

import com.google.gson.Gson;

public class StoredTabletFileTest {

  @Test
  public void fileConversionTest() {
    String s21 = "hdfs://localhost:8020/accumulo/tables/1/t-0000000/A000003v.rf";
    String s31 =
        "{\"path\":\"hdfs://localhost:8020/accumulo/tables/1/t-0000000/A000003v.rf\",\"startRow\":\"\",\"endRow\":\"\"}";
    String s31_untrimmed =
        "   {  \"path\":\"hdfs://localhost:8020/accumulo/tables/1/t-0000000/A000003v.rf\",\"startRow\":\"\",\"endRow\":\"\"  }   ";

    assertTrue(StoredTabletFile.fileNeedsConversion(s21));
    assertFalse(StoredTabletFile.fileNeedsConversion(s31));
    assertFalse(StoredTabletFile.fileNeedsConversion(s31_untrimmed));
  }

  @Test
  public void testSerDe() {
    Gson gson = ByteArrayToBase64TypeAdapter.createBase64Gson();
    String metadataEntry =
        "{ \"path\":\"hdfs://localhost:8020/accumulo//tables//1/t-0000000/A000003v.rf\",\"startRow\":\"AmEA\",\"endRow\":\"AnoA\" }";
    URI normalizedPath =
        URI.create("hdfs://localhost:8020/accumulo/tables/1/t-0000000/A000003v.rf");
    KeyExtent ke = new KeyExtent(TableId.of("t"), new Text("z"), new Text("a"));
    Range r = ke.toDataRange();
    StoredTabletFile expected = new StoredTabletFile(metadataEntry);
    TabletFileCqMetadataGson meta = new TabletFileCqMetadataGson(expected);
    assertEquals(metadataEntry, meta.metadataEntry);
    assertEquals(normalizedPath.toString(), meta.path);
    assertArrayEquals(StoredTabletFile.encodeRow(r.getStartKey()), meta.startRow);
    assertArrayEquals(StoredTabletFile.encodeRow(r.getEndKey()), meta.endRow);
    String json = gson.toJson(meta);
    System.out.println(json);
    TabletFileCqMetadataGson des = gson.fromJson(json, TabletFileCqMetadataGson.class);
    assertEquals(metadataEntry, des.metadataEntry);
    assertEquals(normalizedPath.toString(), des.path);
    assertArrayEquals(StoredTabletFile.encodeRow(r.getStartKey()), des.startRow);
    assertArrayEquals(StoredTabletFile.encodeRow(r.getEndKey()), des.endRow);
  }
}
