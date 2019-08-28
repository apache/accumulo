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
package org.apache.accumulo.master;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.clientImpl.Tables;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.fate.Fate;
import org.apache.accumulo.master.tableOps.TraceRepo;
import org.apache.accumulo.master.tableOps.delete.DeleteTable;
import org.apache.accumulo.server.ServerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DeleteTrashTablesTask implements Runnable {

  private static final Logger log = LoggerFactory.getLogger(DeleteTrashTablesTask.class);

  private final Fate<Master> fate;
  private final ServerContext context;
  private final long period;

  DeleteTrashTablesTask(Fate<Master> fate, ServerContext context, long period) {
    this.fate = fate;
    this.context = context;
    this.period = period;
  }

  @Override
  public void run() {
    Set<String> tables = Tables.getTrashNameToIdMap(context).keySet();
    Set<String> outdatedTables = new HashSet<>();
    Date now = new Date();

    for (String table : tables) {
      Matcher matcher = Constants.TRASH_TABLE_NAME_PATTERN.matcher(table);
      if (matcher.find()) {
        String timestamp = matcher.group(2);
        try {
          Date formatedDate = new SimpleDateFormat(Constants.TRASH_DATE_FORMAT).parse(timestamp);
          Date eol = new Date(formatedDate.getTime() + period);
          if (eol.before(now)) {
            outdatedTables.add(table);
          }
        } catch (ParseException e) {
          log.debug("Potentially ill-formatted trash table name: " + table);
        }
      }
    }

    for (String table : outdatedTables) {
      long tid = fate.startTransaction();
      log.debug("Seeding FATE op to delete trash table " + table + " with tid " + tid);
      try {
        TableId tableId = Tables.getTrashNameToIdMap(context).get(table);
        NamespaceId namespaceId = Tables.getNamespaceId(context, tableId);

        fate.seedTransaction(tid, new TraceRepo<>(new DeleteTable(namespaceId, tableId)), false);
        fate.waitForCompletion(tid);
      } catch (TableNotFoundException e) {
        log.debug("FATE op deleting trash table " + table + " failed", e);
      } finally {
        fate.delete(tid);
      }
      log.debug("FATE op deleting trash table " + table + " finished");
    }
  }
}
