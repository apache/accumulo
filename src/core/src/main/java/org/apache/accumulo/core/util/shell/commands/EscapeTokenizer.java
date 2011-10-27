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
package org.apache.accumulo.core.util.shell.commands;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

/**
 * 
 * EscapeTokenizer - Supports tokenizing with delimiters while being able to escape the delims. String "1,2,3,4" with delims "," = ["1", "2", "3", "4"] String
 * "1\,2,3,4" with delims "," = ["1,2", "3", "4"]
 * 
 * - The escape char '\' only has a special meaning when it is before a delim String "1,\2,3,4" with delims "," = ["1" , "\2", "3", "4"]
 * 
 * - Multiple delims in a row are considered one delim String "1,,,,,,,,,,,,,,2,3,4","," with delims "," = ["1", "2", "3", "4"]
 * 
 */

public class EscapeTokenizer implements Iterable<String> {
  
  private List<String> tokens;
  
  public EscapeTokenizer(String line, String delimeters) {
    this.tokens = new ArrayList<String>();
    preprocess(line, delimeters);
  }
  
  private void preprocess(String line, String delimeters) {
    StringTokenizer st = new StringTokenizer(line, delimeters, true);
    boolean inEscape = false;
    String current = "", prev = "";
    List<String> toks = new ArrayList<String>();
    
    while (st.hasMoreTokens()) {
      current = st.nextToken();
      if (inEscape) {
        prev += current;
        inEscape = false;
      } else {
        inEscape = current.endsWith("\\");
        if (inEscape)
          prev = current.substring(0, current.length() - 1);
        else {
          if (current.length() == 1 && delimeters.contains(current)) {
            if (!prev.isEmpty())
              toks.add(prev);
          } else
            toks.add(prev + current);
          prev = "";
        }
      }
    }
    if (!prev.isEmpty())
      toks.add(prev);
    this.tokens = toks;
  }
  
  @Override
  public Iterator<String> iterator() {
    return this.tokens.iterator();
  }
  
  public int count() {
    return tokens.size();
  }
}
