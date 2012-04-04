/**
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
package org.apache.accumulo.server.test.continuous;

/**
 * 
 */
public class GenSplits {
  public static void main(String[] args) {
    
    if (args.length != 1) {
      System.err.println("Usage: " + GenSplits.class.getName() + " <num tablets>");
      System.exit(-1);
    }
    
    int numTablets = Integer.parseInt(args[0]);
    
    if (numTablets < 1) {
      System.err.println("ERROR: numTablets < 1");
      System.exit(-1);
    }
    
    int numSplits = numTablets - 1;
    long distance = (Long.MAX_VALUE / numTablets) + 1;
    long split = distance;
    for (int i = 0; i < numSplits; i++) {
      
      String s = String.format("%016x", split);
      
      while (s.charAt(s.length() - 1) == '0') {
        s = s.substring(0, s.length() - 1);
      }
      
      System.out.println(s);
      split += distance;
    }
  }
}
