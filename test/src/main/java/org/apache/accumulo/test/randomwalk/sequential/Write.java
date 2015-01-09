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
package org.apache.accumulo.test.randomwalk.sequential;

import static com.google.common.base.Charsets.UTF_8;

import java.util.Properties;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.test.randomwalk.State;
import org.apache.accumulo.test.randomwalk.Test;
import org.apache.hadoop.io.Text;

public class Write extends Test {

  @Override
  public void visit(State state, Properties props) throws Exception {

    BatchWriter bw = state.getMultiTableBatchWriter().getBatchWriter(state.getString("seqTableName"));

    state.set("numWrites", state.getLong("numWrites") + 1);

    Long totalWrites = state.getLong("totalWrites") + 1;
    if (totalWrites % 10000 == 0) {
      log.debug("Total writes: " + totalWrites);
    }
    state.set("totalWrites", totalWrites);

    Mutation m = new Mutation(new Text(String.format("%010d", totalWrites)));
    m.put(new Text("cf"), new Text("cq"), new Value("val".getBytes(UTF_8)));
    bw.addMutation(m);
  }
}
