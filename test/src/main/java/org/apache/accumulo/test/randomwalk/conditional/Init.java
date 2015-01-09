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
package org.apache.accumulo.test.randomwalk.conditional;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;
import java.util.Random;
import java.util.TreeSet;

import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.client.ConditionalWriter.Status;
import org.apache.accumulo.core.data.Condition;
import org.apache.accumulo.core.data.ConditionalMutation;
import org.apache.accumulo.test.randomwalk.State;
import org.apache.accumulo.test.randomwalk.Test;
import org.apache.hadoop.io.Text;

/**
 *
 */
public class Init extends Test {

  @Override
  public void visit(State state, Properties props) throws Exception {

    int numBanks = (Integer) state.get("numBanks");
    int numAccts = (Integer) state.get("numAccts");

    // add some splits to spread ingest out a little
    TreeSet<Text> splits = new TreeSet<Text>();
    for (int i = 1; i < 10; i++)
      splits.add(new Text(Utils.getBank((int) (numBanks * .1 * i))));
    state.getConnector().tableOperations().addSplits((String) state.get("tableName"), splits);
    log.debug("Added splits " + splits);

    ArrayList<Integer> banks = new ArrayList<Integer>();
    for (int i = 0; i < numBanks; i++)
      banks.add(i);
    // shuffle for case when multiple threads are adding banks
    Collections.shuffle(banks, (Random) state.get("rand"));

    ConditionalWriter cw = (ConditionalWriter) state.get("cw");

    for (int i : banks) {
      ConditionalMutation m = new ConditionalMutation(Utils.getBank(i));
      int acceptedCount = 0;
      for (int j = 0; j < numAccts; j++) {
        String cf = Utils.getAccount(j);
        m.addCondition(new Condition(cf, "seq"));
        m.put(cf, "bal", "100");
        m.put(cf, "seq", Utils.getSeq(0));

        if (j % 1000 == 0 && j > 0) {
          Status status = cw.write(m).getStatus();

          while (status == Status.UNKNOWN)
            status = cw.write(m).getStatus();

          if (status == Status.ACCEPTED)
            acceptedCount++;
          m = new ConditionalMutation(Utils.getBank(i));
        }

      }
      if (m.getConditions().size() > 0) {
        Status status = cw.write(m).getStatus();
        while (status == Status.UNKNOWN)
          status = cw.write(m).getStatus();

        if (status == Status.ACCEPTED)
          acceptedCount++;
      }

      log.debug("Added bank " + Utils.getBank(i) + " " + acceptedCount);
    }

  }
}
