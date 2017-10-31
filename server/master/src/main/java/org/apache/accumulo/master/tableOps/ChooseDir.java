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
package org.apache.accumulo.master.tableOps;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.fs.VolumeChooserEnvironment;
import org.apache.hadoop.fs.Path;

class ChooseDir extends MasterRepo {
  private static final long serialVersionUID = 1L;

  private TableInfo tableInfo;

  ChooseDir(TableInfo ti) {
    this.tableInfo = ti;
  }

  @Override
  public long isReady(long tid, Master environment) throws Exception {
    return 0;
  }

  @Override
  public Repo<Master> call(long tid, Master master) throws Exception {
    // Constants.DEFAULT_TABLET_LOCATION has a leading slash prepended to it so we don't need to add one here
    VolumeChooserEnvironment chooserEnv = new VolumeChooserEnvironment(tableInfo.tableId);
    tableInfo.dir = master.getFileSystem().choose(chooserEnv, ServerConstants.getBaseUris()) + Constants.HDFS_TABLES_DIR + Path.SEPARATOR + tableInfo.tableId
        + Constants.DEFAULT_TABLET_LOCATION;
    return new CreateDir(tableInfo);
  }

  @Override
  public void undo(long tid, Master master) throws Exception {

  }
}
