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
package org.apache.accumulo.server.test.randomwalk.image;

import java.security.MessageDigest;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.server.test.randomwalk.State;
import org.apache.accumulo.server.test.randomwalk.Test;
import org.apache.hadoop.io.Text;

public class Write extends Test {
  
  @Override
  public void visit(State state, Properties props) throws Exception {
    
    MultiTableBatchWriter mtbw = state.getMultiTableBatchWriter();
    
    BatchWriter imagesBW = mtbw.getBatchWriter(state.getString("imageTableName"));
    BatchWriter indexBW = mtbw.getBatchWriter(state.getString("indexTableName"));
    
    String uuid = UUID.randomUUID().toString();
    Mutation m = new Mutation(new Text(uuid));
    
    // create a fake image between 4KB and 1MB
    int maxSize = Integer.parseInt(props.getProperty("maxSize"));
    int minSize = Integer.parseInt(props.getProperty("minSize"));
    
    Random rand = new Random();
    int numBytes = rand.nextInt((maxSize - minSize)) + minSize;
    byte[] imageBytes = new byte[numBytes];
    rand.nextBytes(imageBytes);
    m.put(new Text("content"), new Text("image"), new Value(imageBytes));
    
    // store size
    Text meta = new Text("meta");
    m.put(meta, new Text("size"), new Value(String.format("%d", numBytes).getBytes()));
    
    // store hash
    MessageDigest alg = MessageDigest.getInstance("SHA-1");
    alg.update(imageBytes);
    m.put(meta, new Text("sha1"), new Value(alg.digest()));
    
    // update write counts
    state.set("numWrites", state.getInteger("numWrites") + 1);
    Integer totalWrites = state.getInteger("totalWrites") + 1;
    if ((totalWrites % 10000) == 0) {
      log.debug("Total writes: " + totalWrites);
    }
    state.set("totalWrites", totalWrites);
    
    // set count
    m.put(meta, new Text("count"), new Value(String.format("%d", totalWrites).getBytes()));
    
    // add mutation
    imagesBW.addMutation(m);
    
    // now add mutation to index
    alg = MessageDigest.getInstance("SHA-1");
    alg.update(uuid.getBytes());
    Text row = new Text(alg.digest());
    m = new Mutation(row);
    m.put(meta, new Text("uuid"), new Value(uuid.getBytes()));
    
    indexBW.addMutation(m);
    
    Text lastRow = (Text) state.get("lastIndexRow");
    if (lastRow.compareTo(row) < 0) {
      state.set("lastIndexRow", new Text(row));
    }
  }
}
