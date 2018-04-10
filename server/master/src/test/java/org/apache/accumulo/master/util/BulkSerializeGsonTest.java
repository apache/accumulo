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
package org.apache.accumulo.master.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.accumulo.core.client.impl.Bulk;
import org.apache.accumulo.core.client.impl.Bulk.Files;
import org.apache.accumulo.core.client.impl.BulkSerialize;
import org.apache.accumulo.core.client.impl.BulkSerialize.LoadMappingIterator;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.client.impl.Table.ID;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;

public class BulkSerializeGsonTest {
  static File bulkDir;
  static VolumeManager fs;

  @BeforeClass
  public static void setup() throws Exception {
    bulkDir = new File(System.getProperty("user.dir") + "/target/BulkSerializeGsonTest");
    fs = VolumeManagerImpl.getLocal(bulkDir.getAbsolutePath());
  }

  @AfterClass
  public static void cleanup() throws Exception {
    fs.deleteRecursively(new Path(bulkDir.getAbsolutePath()));
  }

  @Test
  public void testUpdateLoadMapping() throws Exception {
    Table.ID tableId = Table.ID.of("1");
    SortedMap<KeyExtent,Bulk.Files> loadMapping = generateMapping(tableId);
    Map<String,String> renames = generateRenames();

    BulkSerialize.writeRenameMap(renames, bulkDir.getAbsolutePath(), tableId, p -> fs.create(p));
    BulkSerialize.writeLoadMapping(loadMapping, bulkDir.getAbsolutePath(), tableId, "test",
        p -> fs.create(p));
    LoadMappingIterator updatedLoadMapping = BulkSerialize
        .getUpdatedLoadMapping(bulkDir.getAbsolutePath(), tableId, p -> fs.open(p));
    updatedLoadMapping.forEachRemaining(entry -> {
      KeyExtent ke = entry.getKey();
      Files files = entry.getValue();
      // System.out.println(ke.toString() + ":" +
      // Arrays.toString(files.getAllFileNames().toArray()));
      loadMapping.get(ke).forEach(origFileInfo -> {
        String newName = renames.get(origFileInfo.getFileName());
        Bulk.FileInfo updatedFile = files.get(newName);
        // make sure new file names aren't null
        assertNotNull("New file name missing from updated mapping", updatedFile);
        assertNotNull("Updated file name is null", updatedFile.getFileName());
        // System.out.println("Updated file name = " + updatedFile.getFileName());
        assertTrue("Updated file name was not changed",
            updatedFile.getFileName().startsWith("new"));
      });
    });
  }

  @Test
  public void parseBulkMapJsonString() {
    ID tableId = Table.ID.of("9");
    SortedMap<KeyExtent,Bulk.Files> mapping = generateMapping(tableId);
    SortedMap<KeyExtent,Bulk.Files> readMapping = new TreeMap<>();
    SortedSet<Bulk.Mapping> loadSet = new TreeSet<>();
    mapping.forEach((ke, files) -> loadSet.add(new Bulk.Mapping(ke, files)));

    Text first = mapping.firstKey().getEndRow();
    Text last = mapping.lastKey().getEndRow();
    assertTrue("Order of bulk mapping is incorrect, first b != " + first,
        first.equals(new Text("b")));
    assertTrue("Order of bulk mapping is incorrect, last d != " + last, last.equals(new Text("d")));

    Gson gson = new GsonBuilder().create();
    String json = gson.toJson(loadSet);

    JsonParser parser = new JsonParser();
    JsonArray array = parser.parse(json).getAsJsonArray();

    for (JsonElement jsonElement : array) {
      Bulk.Mapping bm = gson.fromJson(jsonElement, Bulk.Mapping.class);
      readMapping.put(bm.getKeyExtent(tableId), bm.getFiles());
    }
    assertTrue("Read bulk mapping size " + readMapping.size() + " != 3",
        mapping.size() == readMapping.size());

    KeyExtent firstRead = readMapping.firstKey();
    KeyExtent lastRead = readMapping.lastKey();
    assertEquals("Bulk mapping is incorrect, comparing first keys", mapping.firstKey(), firstRead);
    assertEquals("Bulk mapping is incorrect, comparing last keys ", mapping.lastKey(), lastRead);
  }

  @Test
  public void parseRenameMapJsonString() {
    Map<String,String> renames = generateRenames();

    Gson gson = new GsonBuilder().create();
    String json = gson.toJson(renames);
    Map<String,String> readMap = gson.fromJson(json,
        new TypeToken<Map<String,String>>() {}.getType());

    assertEquals("Read different object then original", renames, readMap);
  }

  private Map<String,String> generateRenames() {
    Map<String,String> renames = new HashMap<>();
    for (String f : "f1 f2 f3 g1 g2 g3 h1 h2 h3".split(" "))
      renames.put(f, "new" + f);
    return renames;
  }

  public SortedMap<KeyExtent,Bulk.Files> generateMapping(Table.ID tableId) {
    SortedMap<KeyExtent,Bulk.Files> mapping = new TreeMap<>();
    Bulk.Files testFiles = new Bulk.Files();
    Bulk.Files testFiles2 = new Bulk.Files();
    Bulk.Files testFiles3 = new Bulk.Files();
    long c = 0L;
    for (String f : "f1 f2 f3".split(" ")) {
      c++;
      testFiles.add(new Bulk.FileInfo(f, c, c));
    }
    c = 0L;
    for (String f : "g1 g2 g3".split(" ")) {
      c++;
      testFiles2.add(new Bulk.FileInfo(f, c, c));
    }
    for (String f : "h1 h2 h3".split(" ")) {
      c++;
      testFiles3.add(new Bulk.FileInfo(f, c, c));
    }

    // add out of order to test sorting
    mapping.put(new KeyExtent(tableId, new Text("d"), new Text("c")), testFiles);
    mapping.put(new KeyExtent(tableId, new Text("c"), new Text("b")), testFiles2);
    mapping.put(new KeyExtent(tableId, new Text("b"), new Text("a")), testFiles3);

    return mapping;
  }
}
