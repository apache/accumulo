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
package org.apache.accumulo.core.replication;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.impl.Namespaces;
import org.apache.accumulo.fate.util.UtilWaitThread;
import org.apache.log4j.Logger;

public class ReplicationTable {
  private static final Logger log = Logger.getLogger(ReplicationTable.class);

  public static final String ID = "_repl";
  public static final String NAME = Namespaces.ACCUMULO_NAMESPACE + ".replication";

  /**
   * Attempt to create the replication table, retrying a limited number of times before failing
   */
  public static synchronized void create(TableOperations tops) {
    int attempts = 5;
    while (attempts > 0 && !tops.exists(NAME)) {
      try {
        tops.create(NAME);
        return;
      } catch (AccumuloException|AccumuloSecurityException e) {
        log.warn("Failed to create " + NAME + ", will retry", e);
        UtilWaitThread.sleep(1000);
      } catch (TableExistsException e) {
        // Already exists, return
        return;
      }
      attempts--;
    }

    throw new RuntimeException("Failed to create replication table after multiple attempts");
  }
}
