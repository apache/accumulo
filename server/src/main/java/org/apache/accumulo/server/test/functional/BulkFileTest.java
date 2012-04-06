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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVWriter;
import org.apache.accumulo.core.file.FileUtil;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.accumulo.server.trace.TraceFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

public class BulkFileTest extends FunctionalTest {
  
  @Override
  public void cleanup() throws Exception {}
  
  @Override
  public Map<String,String> getInitialConfig() {
    return Collections.emptyMap();
  }
  
  @Override
  public List<TableSetup> getTablesToCreate() {
    return Collections.singletonList(new TableSetup("bulkFile", "0333", "0666", "0999", "1333", "1666"));
  }
  
  @Override
  public void run() throws Exception {
    Configuration conf = new Configuration();
    AccumuloConfiguration aconf = ServerConfiguration.getDefaultConfiguration();
    FileSystem fs = TraceFileSystem.wrap(FileUtil.getFileSystem(conf, aconf));
    
    String dir = "/tmp/bulk_test_diff_files_89723987592";
    
    fs.delete(new Path(dir), true);
    
    FileSKVWriter writer1 = FileOperations.getInstance().openWriter(dir + "/f1." + RFile.EXTENSION, fs, conf, aconf);
    writer1.startDefaultLocalityGroup();
    writeData(writer1, 0, 333);
    writer1.close();
    
    FileSKVWriter writer2 = FileOperations.getInstance().openWriter(dir + "/f2." + RFile.EXTENSION, fs, conf, aconf);
    writer2.startDefaultLocalityGroup();
    writeData(writer2, 334, 999);
    writer2.close();
    
    FileSKVWriter writer3 = FileOperations.getInstance().openWriter(dir + "/f3." + RFile.EXTENSION, fs, conf, aconf);
    writer3.startDefaultLocalityGroup();
    writeData(writer3, 1000, 1999);
    writer3.close();
    
    bulkImport(fs, "bulkFile", dir);
    
    checkRFiles("bulkFile", 6, 6, 1, 1);
    
    verifyData("bulkFile", 0, 1999);
    
  }
  
  private void verifyData(String table, int s, int e) throws Exception {
    Scanner scanner = getConnector().createScanner(table, Constants.NO_AUTHS);
    
    Iterator<Entry<Key,Value>> iter = scanner.iterator();
    
    for (int i = s; i <= e; i++) {
      if (!iter.hasNext())
        throw new Exception("row " + i + " not found");
      
      Entry<Key,Value> entry = iter.next();
      
      String row = String.format("%04d", i);
      
      if (!entry.getKey().getRow().equals(new Text(row)))
        throw new Exception("unexpected row " + entry.getKey() + " " + i);
      
      if (Integer.parseInt(entry.getValue().toString()) != i)
        throw new Exception("unexpected value " + entry + " " + i);
    }
    
    if (iter.hasNext())
      throw new Exception("found more than expected " + iter.next());
  }
  
  private void writeData(FileSKVWriter w, int s, int e) throws Exception {
    for (int i = s; i <= e; i++) {
      w.append(new Key(new Text(String.format("%04d", i))), new Value(("" + i).getBytes()));
    }
  }
  
}
