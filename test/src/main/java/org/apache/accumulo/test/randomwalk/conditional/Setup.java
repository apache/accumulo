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
import java.util.Random;

import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.client.ConditionalWriterConfig;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.test.randomwalk.State;
import org.apache.accumulo.test.randomwalk.Test;

public class Setup extends Test {

  @Override
  public void visit(State state, Properties props) throws Exception {
    Random rand = new Random();
    state.set("rand", rand);

    int numBanks = Integer.parseInt(props.getProperty("numBanks", "1000"));
    log.debug("numBanks = " + numBanks);
    state.set("numBanks", numBanks);

    int numAccts = Integer.parseInt(props.getProperty("numAccts", "10000"));
    log.debug("numAccts = " + numAccts);
    state.set("numAccts", numAccts);

    String tableName = "banks";
    state.set("tableName", tableName);

    try {
      state.getConnector().tableOperations().create(tableName);
      log.debug("created table " + tableName);
      boolean blockCache = rand.nextBoolean();
      state.getConnector().tableOperations().setProperty(tableName, Property.TABLE_BLOCKCACHE_ENABLED.getKey(), blockCache + "");
      log.debug("set " + Property.TABLE_BLOCKCACHE_ENABLED.getKey() + " " + blockCache);
    } catch (TableExistsException tee) {}

    ConditionalWriter cw = state.getConnector().createConditionalWriter(tableName, new ConditionalWriterConfig().setMaxWriteThreads(1));
    state.set("cw", cw);

  }
}
