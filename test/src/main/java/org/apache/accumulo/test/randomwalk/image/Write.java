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
package org.apache.accumulo.test.randomwalk.image;

import static com.google.common.base.Charsets.UTF_8;

import java.security.MessageDigest;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.test.randomwalk.State;
import org.apache.accumulo.test.randomwalk.Test;
import org.apache.hadoop.io.Text;

public class Write extends Test {

  static final Text UUID_COLUMN_QUALIFIER = new Text("uuid");
  static final Text COUNT_COLUMN_QUALIFIER = new Text("count");
  static final Text SHA1_COLUMN_QUALIFIER = new Text("sha1");
  static final Text IMAGE_COLUMN_QUALIFIER = new Text("image");
  static final Text META_COLUMN_FAMILY = new Text("meta");
  static final Text CONTENT_COLUMN_FAMILY = new Text("content");

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
    int numBytes = rand.nextInt(maxSize - minSize) + minSize;
    byte[] imageBytes = new byte[numBytes];
    rand.nextBytes(imageBytes);
    m.put(CONTENT_COLUMN_FAMILY, IMAGE_COLUMN_QUALIFIER, new Value(imageBytes));

    // store size
    m.put(META_COLUMN_FAMILY, new Text("size"), new Value(String.format("%d", numBytes).getBytes(UTF_8)));

    // store hash
    MessageDigest alg = MessageDigest.getInstance("SHA-1");
    alg.update(imageBytes);
    byte[] hash = alg.digest();
    m.put(META_COLUMN_FAMILY, SHA1_COLUMN_QUALIFIER, new Value(hash));

    // update write counts
    state.set("numWrites", state.getLong("numWrites") + 1);
    Long totalWrites = state.getLong("totalWrites") + 1;
    state.set("totalWrites", totalWrites);

    // set count
    m.put(META_COLUMN_FAMILY, COUNT_COLUMN_QUALIFIER, new Value(String.format("%d", totalWrites).getBytes(UTF_8)));

    // add mutation
    imagesBW.addMutation(m);

    // now add mutation to index
    Text row = new Text(hash);
    m = new Mutation(row);
    m.put(META_COLUMN_FAMILY, UUID_COLUMN_QUALIFIER, new Value(uuid.getBytes(UTF_8)));

    indexBW.addMutation(m);

    Text lastRow = (Text) state.get("lastIndexRow");
    if (lastRow.compareTo(row) < 0) {
      state.set("lastIndexRow", new Text(row));
    }
  }
}
