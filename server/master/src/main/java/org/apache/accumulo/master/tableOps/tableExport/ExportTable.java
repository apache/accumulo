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
package org.apache.accumulo.master.tableOps.tableExport;

import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.master.tableOps.MasterRepo;
import org.apache.accumulo.master.tableOps.Utils;
import org.apache.hadoop.fs.Path;

public class ExportTable extends MasterRepo {
  private static final long serialVersionUID = 1L;

  private final ExportInfo tableInfo;

  public ExportTable(NamespaceId namespaceId, String tableName, TableId tableId, String exportDir) {
    tableInfo = new ExportInfo();
    tableInfo.tableName = tableName;
    tableInfo.exportDir = exportDir;
    tableInfo.tableID = tableId;
    tableInfo.namespaceID = namespaceId;
  }

  @Override
  public long isReady(long tid, Master environment) throws Exception {
    return Utils.reserveHdfsDirectory(environment, new Path(tableInfo.exportDir).toString(), tid);
  }

  @Override
  public Repo<Master> call(long tid, Master env) {
    return new WriteExportFiles(tableInfo);
  }

  @Override
  public void undo(long tid, Master env) throws Exception {
    Utils.unreserveHdfsDirectory(env, new Path(tableInfo.exportDir).toString(), tid);
  }

  public static final int VERSION = 1;

  public static final String DATA_VERSION_PROP = "srcDataVersion";
  public static final String EXPORT_VERSION_PROP = "exportVersion";

}
