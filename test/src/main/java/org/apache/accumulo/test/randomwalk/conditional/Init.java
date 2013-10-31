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

import java.util.Properties;

import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.data.Condition;
import org.apache.accumulo.core.data.ConditionalMutation;
import org.apache.accumulo.test.randomwalk.State;
import org.apache.accumulo.test.randomwalk.Test;

/**
 * 
 */
public class Init extends Test {

  @Override
  public void visit(State state, Properties props) throws Exception {

    int numBanks = (Integer) state.get("numBanks");
    int numAccts = (Integer) state.get("numAccts");
    ConditionalWriter cw = (ConditionalWriter) state.get("cw");

    for (int i = 0; i < numBanks; i++) {
      ConditionalMutation m = null;
      for (int j = 0; j < numAccts; j++) {
        String cf = Utils.getAccount(j);
        if (m == null) {
          m = new ConditionalMutation(Utils.getBank(i), new Condition(cf, "seq"));
        } else {
          m.addCondition(new Condition(cf, "seq"));
        }
        m.put(cf, "bal", "100");
        m.put(cf, "seq", Utils.getSeq(0));

        if (j % 1000 == 0) {
          cw.write(m);
          m = null;
        }

      }
      if (m != null)
        cw.write(m);

      log.debug("Added bank " + Utils.getBank(i));
    }

  }
}
