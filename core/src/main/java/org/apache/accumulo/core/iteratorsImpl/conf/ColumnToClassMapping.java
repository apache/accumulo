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
package org.apache.accumulo.core.iteratorsImpl.conf;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.classloader.ClassLoaderUtil;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.iteratorsImpl.conf.ColumnUtil.ColFamHashKey;
import org.apache.accumulo.core.iteratorsImpl.conf.ColumnUtil.ColHashKey;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.io.Text;

public class ColumnToClassMapping<K> {

  private HashMap<ColFamHashKey,K> objectsCF;
  private HashMap<ColHashKey,K> objectsCol;

  private ColHashKey lookupCol = new ColHashKey();
  private ColFamHashKey lookupCF = new ColFamHashKey();

  public ColumnToClassMapping() {
    objectsCF = new HashMap<>();
    objectsCol = new HashMap<>();
  }

  public ColumnToClassMapping(Map<String,String> objectStrings, Class<? extends K> c)
      throws ReflectiveOperationException, IOException {
    this(objectStrings, c, null);
  }

  public ColumnToClassMapping(Map<String,String> objectStrings, Class<? extends K> c,
      String context) throws ReflectiveOperationException, IOException {
    this();

    for (Entry<String,String> entry : objectStrings.entrySet()) {
      String column = entry.getKey();
      String className = entry.getValue();

      Pair<Text,Text> pcic = ColumnSet.decodeColumns(column);

      Class<? extends K> clazz = ClassLoaderUtil.loadClass(context, className, c);

      K inst = clazz.getDeclaredConstructor().newInstance();
      if (pcic.getSecond() == null) {
        addObject(pcic.getFirst(), inst);
      } else {
        addObject(pcic.getFirst(), pcic.getSecond(), inst);
      }
    }
  }

  protected void addObject(Text colf, K obj) {
    objectsCF.put(new ColFamHashKey(new Text(colf)), obj);
  }

  protected void addObject(Text colf, Text colq, K obj) {
    objectsCol.put(new ColHashKey(colf, colq), obj);
  }

  public K getObject(Key key) {
    K obj = null;

    // lookup column family and column qualifier
    if (!objectsCol.isEmpty()) {
      lookupCol.set(key);
      obj = objectsCol.get(lookupCol);
      if (obj != null) {
        return obj;
      }
    }

    // lookup just column family
    if (!objectsCF.isEmpty()) {
      lookupCF.set(key);
      obj = objectsCF.get(lookupCF);
    }

    return obj;
  }

  public boolean isEmpty() {
    return objectsCol.isEmpty() && objectsCF.isEmpty();
  }
}
