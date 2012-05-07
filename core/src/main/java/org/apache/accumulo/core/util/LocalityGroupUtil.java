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
/**
 * 
 */
package org.apache.accumulo.core.util;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Column;
import org.apache.hadoop.io.Text;

public class LocalityGroupUtil {
  
  // private static final Logger log = Logger.getLogger(ColumnFamilySet.class);
  
  public static Set<ByteSequence> EMPTY_CF_SET = Collections.emptySet();
  
  public static Set<ByteSequence> families(Collection<Column> columns) {
    Set<ByteSequence> result = new HashSet<ByteSequence>(columns.size());
    for (Column col : columns) {
      result.add(new ArrayByteSequence(col.getColumnFamily()));
    }
    return result;
  }
  
  @SuppressWarnings("serial")
  static public class LocalityGroupConfigurationError extends AccumuloException {
    LocalityGroupConfigurationError(String why) {
      super(why);
    }
  }
  
  public static Map<String,Set<ByteSequence>> getLocalityGroups(AccumuloConfiguration acuconf) throws LocalityGroupConfigurationError {
    Map<String,Set<ByteSequence>> result = new HashMap<String,Set<ByteSequence>>();
    String[] groups = acuconf.get(Property.TABLE_LOCALITY_GROUPS).split(",");
    for (String group : groups) {
      if (group.length() > 0)
        result.put(group, new HashSet<ByteSequence>());
    }
    HashSet<ByteSequence> all = new HashSet<ByteSequence>();
    for (Entry<String,String> entry : acuconf) {
      String property = entry.getKey();
      String value = entry.getValue();
      String prefix = Property.TABLE_LOCALITY_GROUP_PREFIX.getKey();
      if (property.startsWith(prefix)) {
        // this property configures a locality group, find out which one:
        String group = property.substring(prefix.length());
        String[] parts = group.split("\\.");
        group = parts[0];
        if (result.containsKey(group)) {
          if (parts.length == 1) {
            Set<ByteSequence> colFamsSet = decodeColumnFamilies(value);
            if (!Collections.disjoint(all, colFamsSet)) {
              colFamsSet.retainAll(all);
              throw new LocalityGroupConfigurationError("Column families " + colFamsSet + " in group " + group + " is already used by another locality group");
            }
            
            all.addAll(colFamsSet);
            result.put(group, colFamsSet);
          }
        }
      }
    }
    // result.put("", all);
    return result;
  }
  
  public static Set<ByteSequence> decodeColumnFamilies(String colFams) throws LocalityGroupConfigurationError {
    HashSet<ByteSequence> colFamsSet = new HashSet<ByteSequence>();
    
    for (String family : colFams.split(",")) {
      ByteSequence cfbs = decodeColumnFamily(family);
      colFamsSet.add(cfbs);
    }
    
    return colFamsSet;
  }
  
  public static ByteSequence decodeColumnFamily(String colFam) throws LocalityGroupConfigurationError {
    byte output[] = new byte[colFam.length()];
    int pos = 0;
    
    for (int i = 0; i < colFam.length(); i++) {
      char c = colFam.charAt(i);
      
      if (c == '\\') {
        // next char must be 'x' or '\'
        i++;
        
        if (i >= colFam.length()) {
          throw new LocalityGroupConfigurationError("Expected 'x' or '\' after '\'  in " + colFam);
        }
        
        char nc = colFam.charAt(i);
        
        switch (nc) {
          case '\\':
            output[pos++] = '\\';
            break;
          case 'x':
            // next two chars must be [0-9][0-9]
            i++;
            output[pos++] = (byte) (0xff & Integer.parseInt(colFam.substring(i, i + 2), 16));
            i++;
            break;
          default:
            throw new LocalityGroupConfigurationError("Expected 'x' or '\' after '\'  in " + colFam);
        }
      } else {
        output[pos++] = (byte) (0xff & c);
      }
      
    }
    
    return new ArrayByteSequence(output, 0, pos);
    
  }
  
  public static String encodeColumnFamilies(Set<Text> colFams) {
    HashSet<String> ecfs = new HashSet<String>();
    
    StringBuilder sb = new StringBuilder();
    
    for (Text text : colFams) {
      String ecf = encodeColumnFamily(sb, text.getBytes(), 0, text.getLength());
      ecfs.add(ecf);
    }
    
    return StringUtil.join(ecfs, ",");
  }
  
  public static String encodeColumnFamily(ByteSequence bs) {
    return encodeColumnFamily(new StringBuilder(), bs.getBackingArray(), bs.offset(), bs.length());
  }
  
  private static String encodeColumnFamily(StringBuilder sb, byte[] ba, int offset, int len) {
    sb.setLength(0);
    
    for (int i = 0; i < len; i++) {
      int c = 0xff & ba[i];
      if (c == '\\')
        sb.append("\\\\");
      else if (c >= 32 && c <= 126 && c != ',')
        sb.append((char) c);
      else
        sb.append("\\x").append(String.format("%02X", c));
    }
    
    String ecf = sb.toString();
    return ecf;
  }
  
}
