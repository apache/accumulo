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

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.test.randomwalk.State;
import org.apache.accumulo.test.randomwalk.Test;

public class RenameTable extends Test {

  @Override
  public void visit(State state, Properties props) throws Exception {
    Connector conn = state.getConnector();

    Random rand = (Random) state.get("rand");

    @SuppressWarnings("unchecked")
    List<String> tableNames = (List<String>) state.get("tables");

    String srcTableName = tableNames.get(rand.nextInt(tableNames.size()));
    String newTableName = tableNames.get(rand.nextInt(tableNames.size()));

    String srcNamespace = "", newNamespace = "";

    int index = srcTableName.indexOf('.');
    if (-1 != index) {
      srcNamespace = srcTableName.substring(0, index);
    }

    index = newTableName.indexOf('.');
    if (-1 != index) {
      newNamespace = newTableName.substring(0, index);
    }

    try {
      conn.tableOperations().rename(srcTableName, newTableName);
      log.debug("Renamed table " + srcTableName + " " + newTableName);
    } catch (TableExistsException e) {
      log.debug("Rename " + srcTableName + " failed, " + newTableName + " exists");
    } catch (TableNotFoundException e) {
      Throwable cause = e.getCause();
      if (null != cause) {
        // Rename has to have failed on the destination namespace, because the source namespace
        // couldn't be deleted with our table in it
        if (cause.getClass().isAssignableFrom(NamespaceNotFoundException.class)) {
          log.debug("Rename failed because new namespace doesn't exist: " + newNamespace, cause);
          // Avoid the final src/dest namespace check
          return;
        }
      }

      log.debug("Rename " + srcTableName + " failed, doesnt exist");
    } catch (IllegalArgumentException e) {
      log.debug("Rename: " + e.toString());
    } catch (AccumuloException e) {
      // Catch the expected failure when we try to rename a table into a new namespace
      if (!srcNamespace.equals(newNamespace)) {
        return;
      }
      log.debug("Rename " + srcTableName + " failed.", e);
    }

    if (!srcNamespace.equals(newNamespace)) {
      log.error("RenameTable operation should have failed when renaming across namespaces.");
    }
  }
}
