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
package org.apache.accumulo.test.functional;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.accumulo.test.TestIngest;
import org.apache.hadoop.io.Text;

/**
 * See ACCUMULO-779
 */
public class FateStarvationTest extends FunctionalTest {
  
  @Override
  public void cleanup() throws Exception {}
  
  @Override
  public Map<String,String> getInitialConfig() {
    return Collections.emptyMap();
  }
  
  @Override
  public List<TableSetup> getTablesToCreate() {
    return Collections.emptyList();
  }
  
  @Override
  public void run() throws Exception {
    getConnector().tableOperations().create("test_ingest");
    
    getConnector().tableOperations().addSplits("test_ingest", TestIngest.getSplitPoints(0, 100000, 50));
    
    TestIngest.main(new String[] {"-random", "89", "-timestamp", "7", "-size", "" + 50, "100000", "0", "1"});
    
    getConnector().tableOperations().flush("test_ingest", null, null, true);
    
    List<Text> splits = new ArrayList<Text>(TestIngest.getSplitPoints(0, 100000, 67));
    Random rand = new Random();
    
    for (int i = 0; i < 100; i++) {
      int idx1 = rand.nextInt(splits.size() - 1);
      int idx2 = rand.nextInt(splits.size() - (idx1 + 1)) + idx1 + 1;
      
      getConnector().tableOperations().compact("test_ingest", splits.get(idx1), splits.get(idx2), false, false);
    }
    
    getConnector().tableOperations().offline("test_ingest");
  }
  
  public static void main(String[] args) throws Exception {
    ArrayList<String> argsList = new ArrayList<String>();
    argsList.addAll(Arrays.asList(args));
    argsList.addAll(Arrays.asList(FateStarvationTest.class.getName(), "run"));
    FunctionalTest.main(argsList.toArray(new String[0]));
  }

}
