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
package org.apache.accumulo.examples.wikisearch.util;

import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.data.Key;

public class BaseKeyParser {
  public static final String ROW_FIELD = "row";
  public static final String COLUMN_FAMILY_FIELD = "columnFamily";
  public static final String COLUMN_QUALIFIER_FIELD = "columnQualifier";
  
  protected Map<String,String> keyFields = new HashMap<String,String>();
  protected Key key = null;
  
  /**
   * Parses a Key object into its constituent fields. This method clears any prior values, so the object can be reused without requiring a new instantiation.
   * This default implementation makes the row, columnFamily, and columnQualifier available.
   * 
   * @param key
   */
  public void parse(Key key) {
    this.key = key;
    
    keyFields.clear();
    
    keyFields.put(ROW_FIELD, key.getRow().toString());
    keyFields.put(COLUMN_FAMILY_FIELD, key.getColumnFamily().toString());
    keyFields.put(COLUMN_QUALIFIER_FIELD, key.getColumnQualifier().toString());
  }
  
  public String getFieldValue(String fieldName) {
    return keyFields.get(fieldName);
  }
  
  public String[] getFieldNames() {
    String[] fieldNames = new String[keyFields.size()];
    return keyFields.keySet().toArray(fieldNames);
  }
  
  public BaseKeyParser duplicate() {
    return new BaseKeyParser();
  }
  
  public String getRow() {
    return keyFields.get(ROW_FIELD);
  }
  
  public String getColumnFamily() {
    return keyFields.get(COLUMN_FAMILY_FIELD);
  }
  
  public String getColumnQualifier() {
    return keyFields.get(COLUMN_QUALIFIER_FIELD);
  }
  
  public Key getKey() {
    return this.key;
  }
  
}
