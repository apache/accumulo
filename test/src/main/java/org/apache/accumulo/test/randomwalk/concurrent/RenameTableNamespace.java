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
package org.apache.accumulo.test.randomwalk.concurrent;

import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNamespaceExistsException;
import org.apache.accumulo.core.client.TableNamespaceNotFoundException;
import org.apache.accumulo.test.randomwalk.State;
import org.apache.accumulo.test.randomwalk.Test;

public class RenameTableNamespace extends Test {
  
  @Override
  public void visit(State state, Properties props) throws Exception {
    Connector conn = state.getConnector();
    
    Random rand = (Random) state.get("rand");
    
    @SuppressWarnings("unchecked")
    List<String> namespaces = (List<String>) state.get("namespaces");
    
    String srcName = namespaces.get(rand.nextInt(namespaces.size()));
    String newName = namespaces.get(rand.nextInt(namespaces.size()));
    
    try {
      conn.tableNamespaceOperations().rename(srcName, newName);
      log.debug("Renamed table namespace " + srcName + " " + newName);
    } catch (TableNamespaceExistsException e) {
      log.debug("Rename namespace " + srcName + " failed, " + newName + " exists");
    } catch (TableNamespaceNotFoundException e) {
      log.debug("Rename namespace " + srcName + " failed, doesnt exist");
    }
  }
}
