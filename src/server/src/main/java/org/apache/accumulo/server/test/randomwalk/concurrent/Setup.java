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
package org.apache.accumulo.server.test.randomwalk.concurrent;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.accumulo.server.test.randomwalk.State;
import org.apache.accumulo.server.test.randomwalk.Test;

public class Setup extends Test {
  
  @Override
  public void visit(State state, Properties props) throws Exception {
    Random rand = new Random();
    state.set("rand", rand);
    
    int numTables = Integer.parseInt(props.getProperty("numTables", "5"));
    log.debug("numTables = " + numTables);
    List<String> tables = new ArrayList<String>();
    for (int i = 0; i < numTables; i++)
      tables.add(String.format("ctt_%03d", i));
    state.set("tables", tables);
    
    int numUsers = Integer.parseInt(props.getProperty("numUsers", "5"));
    log.debug("numUsers = " + numUsers);
    List<String> users = new ArrayList<String>();
    for (int i = 0; i < numUsers; i++)
      users.add(String.format("user%03d", i));
    state.set("users", users);
  }
  
}
