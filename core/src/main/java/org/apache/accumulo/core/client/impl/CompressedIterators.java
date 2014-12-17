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
package org.apache.accumulo.core.client.impl;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.thrift.IterInfo;
import org.apache.accumulo.core.util.UnsynchronizedBuffer;

public class CompressedIterators {
  private Map<String,Integer> symbolMap;
  private List<String> symbolTable;
  private Map<ByteSequence,IterConfig> cache;
  
  public static class IterConfig {
    public List<IterInfo> ssiList = new ArrayList<IterInfo>();
    public Map<String,Map<String,String>> ssio = new HashMap<String,Map<String,String>>();
  }

  public CompressedIterators() {
    symbolMap = new HashMap<String,Integer>();
    symbolTable = new ArrayList<String>();
  }
  
  public CompressedIterators(List<String> symbols) {
    this.symbolTable = symbols;
    this.cache = new HashMap<ByteSequence,IterConfig>();
  }

  private int getSymbolID(String symbol) {
    Integer id = symbolMap.get(symbol);
    if (id == null) {
      id = symbolTable.size();
      symbolTable.add(symbol);
      symbolMap.put(symbol, id);
    }
    
    return id;
  }
  
  public ByteBuffer compress(IteratorSetting[] iterators) {
    
    UnsynchronizedBuffer.Writer out = new UnsynchronizedBuffer.Writer(iterators.length * 8);
    
    out.writeVInt(iterators.length);

    for (IteratorSetting is : iterators) {
      out.writeVInt(getSymbolID(is.getName()));
      out.writeVInt(getSymbolID(is.getIteratorClass()));
      out.writeVInt(is.getPriority());
      
      Map<String,String> opts = is.getOptions();
      out.writeVInt(opts.size());
      
      for (Entry<String,String> entry : opts.entrySet()) {
        out.writeVInt(getSymbolID(entry.getKey()));
        out.writeVInt(getSymbolID(entry.getValue()));
      }
    }
    
    return out.toByteBuffer();
    
  }
  
  public IterConfig decompress(ByteBuffer iterators) {
    
    ByteSequence iterKey = new ArrayByteSequence(iterators);
    IterConfig config = cache.get(iterKey);
    if (config != null) {
      return config;
    }

    config = new IterConfig();

    UnsynchronizedBuffer.Reader in = new UnsynchronizedBuffer.Reader(iterators);

    int num = in.readVInt();
    
    for (int i = 0; i < num; i++) {
      String name = symbolTable.get(in.readVInt());
      String iterClass = symbolTable.get(in.readVInt());
      int prio = in.readVInt();
      
      config.ssiList.add(new IterInfo(prio, iterClass, name));
      
      int numOpts = in.readVInt();
      
      HashMap<String,String> opts = new HashMap<String,String>();
      
      for (int j = 0; j < numOpts; j++) {
        String key = symbolTable.get(in.readVInt());
        String val = symbolTable.get(in.readVInt());
        
        opts.put(key, val);
      }
      
      config.ssio.put(name, opts);
      
    }

    cache.put(iterKey, config);
    return config;
  }

  public List<String> getSymbolTable() {
    return symbolTable;
  }

}
