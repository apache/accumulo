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
package org.apache.accumulo.server.test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class CreateRFiles {
  
  public static void main(String[] args) {
    String dir = args[0];
    int numThreads = Integer.parseInt(args[1]);
    long start = Long.parseLong(args[2]);
    long end = Long.parseLong(args[3]);
    long numsplits = Long.parseLong(args[4]);
    
    long splitSize = Math.round((end - start) / (double) numsplits);
    
    long currStart = start;
    long currEnd = start + splitSize;
    
    ExecutorService threadPool = Executors.newFixedThreadPool(numThreads);
    
    int count = 0;
    while (currEnd <= end && currStart < currEnd) {
      
      final String tia = String.format("-rFile /%s/mf%05d -timestamp 1 -size 50 -random 56 %d %d 1", dir, count, currEnd - currStart, currStart);
      
      Runnable r = new Runnable() {
        
        @Override
        public void run() {
          try {
            TestIngest.main(tia.split(" "));
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
        
      };
      
      threadPool.execute(r);
      
      count++;
      currStart = currEnd;
      currEnd = Math.min(end, currStart + splitSize);
    }
    
    threadPool.shutdown();
    while (!threadPool.isTerminated())
      try {
        threadPool.awaitTermination(1, TimeUnit.HOURS);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
  }
}
