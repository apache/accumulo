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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.impl.AcceptableThriftTableOperationException;
import org.apache.accumulo.core.client.impl.thrift.TableOperation;
import org.apache.accumulo.core.client.impl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.tablets.UniqueNameAllocator;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class MapImportFileNames extends MasterRepo {
  private static final Logger log = LoggerFactory.getLogger(MapImportFileNames.class);

  private static final long serialVersionUID = 1L;

  private ImportedTableInfo tableInfo;

  MapImportFileNames(ImportedTableInfo ti) {
    this.tableInfo = ti;
  }

  @Override
  public Repo<Master> call(long tid, Master environment) throws Exception {

    Path path = new Path(tableInfo.importDir, "mappings.txt");

    BufferedWriter mappingsWriter = null;

    try {
      VolumeManager fs = environment.getFileSystem();

      fs.mkdirs(new Path(tableInfo.importDir));

      FileStatus[] files = fs.listStatus(new Path(tableInfo.exportDir));

      UniqueNameAllocator namer = UniqueNameAllocator.getInstance();

      mappingsWriter = new BufferedWriter(new OutputStreamWriter(fs.create(path), UTF_8));

      for (FileStatus fileStatus : files) {
        String fileName = fileStatus.getPath().getName();
        log.info("filename " + fileStatus.getPath().toString());
        String sa[] = fileName.split("\\.");
        String extension = "";
        if (sa.length > 1) {
          extension = sa[sa.length - 1];

          if (!FileOperations.getValidExtensions().contains(extension)) {
            continue;
          }
        } else {
          // assume it is a map file
          extension = Constants.MAPFILE_EXTENSION;
        }

        String newName = "I" + namer.getNextName() + "." + extension;

        mappingsWriter.append(fileName);
        mappingsWriter.append(':');
        mappingsWriter.append(newName);
        mappingsWriter.newLine();
      }

      mappingsWriter.close();
      mappingsWriter = null;

      return new PopulateMetadataTable(tableInfo);
    } catch (IOException ioe) {
      log.warn("{}", ioe.getMessage(), ioe);
      throw new AcceptableThriftTableOperationException(tableInfo.tableId.canonicalID(), tableInfo.tableName, TableOperation.IMPORT,
          TableOperationExceptionType.OTHER, "Error writing mapping file " + path + " " + ioe.getMessage());
    } finally {
      if (mappingsWriter != null)
        try {
          mappingsWriter.close();
        } catch (IOException ioe) {
          log.warn("Failed to close " + path, ioe);
        }
    }
  }

  @Override
  public void undo(long tid, Master env) throws Exception {
    env.getFileSystem().deleteRecursively(new Path(tableInfo.importDir));
  }
}
