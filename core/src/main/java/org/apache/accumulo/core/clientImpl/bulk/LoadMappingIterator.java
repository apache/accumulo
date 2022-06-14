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
package org.apache.accumulo.core.clientImpl.bulk;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.core.clientImpl.bulk.BulkSerialize.createGson;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;

/**
 * Iterator for reading the Bulk Load Mapping JSON.
 */
public class LoadMappingIterator
    implements Iterator<Map.Entry<KeyExtent,Bulk.Files>>, AutoCloseable {
  private TableId tableId;
  private JsonReader reader;
  private Gson gson = createGson();
  private Map<String,String> renameMap;

  LoadMappingIterator(TableId tableId, InputStream loadMapFile) throws IOException {
    this.tableId = tableId;
    this.reader = new JsonReader(new BufferedReader(new InputStreamReader(loadMapFile, UTF_8)));
    this.reader.beginArray();
  }

  public void setRenameMap(Map<String,String> renameMap) {
    this.renameMap = renameMap;
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }

  @Override
  public boolean hasNext() {
    try {
      return reader.hasNext();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public Map.Entry<KeyExtent,Bulk.Files> next() {
    Bulk.Mapping bm = gson.fromJson(reader, Bulk.Mapping.class);
    if (renameMap != null) {
      return new AbstractMap.SimpleEntry<>(bm.getKeyExtent(tableId),
          bm.getFiles().mapNames(renameMap));
    } else {
      return new AbstractMap.SimpleEntry<>(bm.getKeyExtent(tableId), bm.getFiles());
    }
  }

}
