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
package org.apache.accumulo.server.test.functional;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.server.test.CreateRFiles;
import org.apache.accumulo.server.test.VerifyIngest;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * This test verifies that when a lot of files are bulk imported into a table with one tablet and then splits that not all map files go to the children tablets.
 * 
 * 
 * 
 */

public class BulkSplitOptimizationTest extends FunctionalTest {
  
  private static final String TABLE_NAME = "test_ingest";
  
  @Override
  public void cleanup() throws Exception {
    FileSystem fs = FileSystem.get(CachedConfiguration.getInstance());
    fs.delete(new Path("/tmp/testmf"), true);
    fs.delete(new Path("/tmp/testmf_failures"), true);
  }
  
  @Override
  public Map<String,String> getInitialConfig() {
    return parseConfig(Property.TSERV_MAJC_DELAY + "=1s");
  }
  
  @Override
  public List<TableSetup> getTablesToCreate() {
    return Collections.singletonList(new TableSetup(TABLE_NAME, parseConfig(Property.TABLE_MAJC_RATIO + "=1000", Property.TABLE_FILE_MAX + "=1000",
        Property.TABLE_SPLIT_THRESHOLD + "=1G")));
  }
  
  @Override
  public void run() throws Exception {
    
    FileSystem fs = FileSystem.get(CachedConfiguration.getInstance());
    fs.delete(new Path("/tmp/testmf"), true);
    
    CreateRFiles.main(new String[] {"tmp/testmf", "8", "0", "100000", "99"});
    
    bulkImport(fs, TABLE_NAME, "/tmp/testmf");
    
    checkSplits(TABLE_NAME, 0, 0);
    checkRFiles(TABLE_NAME, 1, 1, 100, 100);
    
    // initiate splits
    getConnector().tableOperations().setProperty(TABLE_NAME, Property.TABLE_SPLIT_THRESHOLD.getKey(), "100K");
    
    UtilWaitThread.sleep(2000);
    
    // wait until over split threshold
    while (getConnector().tableOperations().getSplits(TABLE_NAME).size() < 50) {
      UtilWaitThread.sleep(500);
    }
    
    checkSplits(TABLE_NAME, 50, 100);
    
    VerifyIngest.main(new String[] {"-timestamp", "1", "-size", "50", "-random", "56", "100000", "0", "1"});
    
    // ensure each tablet does not have all map files
    checkRFiles(TABLE_NAME, 50, 100, 1, 4);
  }
}
