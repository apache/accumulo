/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.manager.tableOps.tableImport;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.core.Constants.IMPORT_MAPPINGS_FILE;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.clientImpl.AcceptableThriftTableOperationException;
import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.clientImpl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily;
import org.apache.accumulo.core.util.FastFormat;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.util.MetadataTableUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PopulateMetadataTable extends ManagerRepo {
  private static final Logger log = LoggerFactory.getLogger(PopulateMetadataTable.class);

  private static final long serialVersionUID = 1L;

  private final ImportedTableInfo tableInfo;

  PopulateMetadataTable(ImportedTableInfo ti) {
    this.tableInfo = ti;
  }

  static void readMappingFile(VolumeManager fs, ImportedTableInfo tableInfo, String importDir,
      Map<String,String> fileNameMappings) throws Exception {
    try (var fsDis = fs.open(new Path(importDir, IMPORT_MAPPINGS_FILE));
        var isr = new InputStreamReader(fsDis, UTF_8);
        BufferedReader in = new BufferedReader(isr)) {
      String line, prev;
      while ((line = in.readLine()) != null) {
        String[] sa = line.split(":", 2);
        prev = fileNameMappings.put(sa[0], importDir + "/" + sa[1]);

        if (prev != null) {
          String msg = "File exists in multiple import directories: '"
              + sa[0].replaceAll("[\r\n]", "") + "'";
          log.warn(msg);
          throw new AcceptableThriftTableOperationException(tableInfo.tableId.canonical(),
              tableInfo.tableName, TableOperation.IMPORT, TableOperationExceptionType.OTHER, msg);
        }
      }
    }
  }

  @Override
  public Repo<Manager> call(long tid, Manager manager) throws Exception {

    Path path = new Path(tableInfo.exportFile);

    VolumeManager fs = manager.getVolumeManager();

    try (BatchWriter mbw = manager.getContext().createBatchWriter(MetadataTable.NAME);
        ZipInputStream zis = new ZipInputStream(fs.open(path))) {

      Map<String,String> fileNameMappings = new HashMap<>();
      for (ImportedTableInfo.DirectoryMapping dm : tableInfo.directories) {
        log.info("importDir is " + dm.importDir);
        // mappings are prefixed with the proper volume information, e.g:
        // hdfs://localhost:8020/path/to/accumulo/tables/...
        readMappingFile(fs, tableInfo, dm.importDir, fileNameMappings);
      }

      ZipEntry zipEntry;
      while ((zipEntry = zis.getNextEntry()) != null) {
        if (zipEntry.getName().equals(Constants.EXPORT_METADATA_FILE)) {
          DataInputStream in = new DataInputStream(new BufferedInputStream(zis));

          Key key = new Key();
          Value val = new Value();

          Mutation m = null;
          Text currentRow = null;
          int dirCount = 0;

          while (true) {
            key.readFields(in);
            val.readFields(in);

            Text endRow = KeyExtent.fromMetaRow(key.getRow()).endRow();
            Text metadataRow = new KeyExtent(tableInfo.tableId, endRow, null).toMetaRow();

            Text cq;

            if (key.getColumnFamily().equals(DataFileColumnFamily.NAME)) {
              String oldName = new Path(key.getColumnQualifier().toString()).getName();
              String newName = fileNameMappings.get(oldName);

              if (newName == null) {
                throw new AcceptableThriftTableOperationException(tableInfo.tableId.canonical(),
                    tableInfo.tableName, TableOperation.IMPORT, TableOperationExceptionType.OTHER,
                    "File " + oldName + " does not exist in import dir");
              }

              cq = new Text(newName);
            } else {
              cq = key.getColumnQualifier();
            }

            if (m == null || !currentRow.equals(metadataRow)) {

              if (m != null) {
                mbw.addMutation(m);
              }

              // Make a unique directory inside the table's dir. Cannot import multiple tables
              // into one table, so don't need to use unique allocator
              String tabletDir = new String(
                  FastFormat.toZeroPaddedString(dirCount++, 8, 16, Constants.CLONE_PREFIX_BYTES),
                  UTF_8);

              m = new Mutation(metadataRow);
              ServerColumnFamily.DIRECTORY_COLUMN.put(m, new Value(tabletDir));
              currentRow = metadataRow;
            }

            m.put(key.getColumnFamily(), cq, val);

            if (endRow == null && TabletColumnFamily.PREV_ROW_COLUMN.hasColumns(key)) {
              mbw.addMutation(m);
              break; // its the last column in the last row
            }
          }
          break;
        }
      }

      return new MoveExportedFiles(tableInfo);
    } catch (IOException ioe) {
      log.warn("{}", ioe.getMessage(), ioe);
      throw new AcceptableThriftTableOperationException(tableInfo.tableId.canonical(),
          tableInfo.tableName, TableOperation.IMPORT, TableOperationExceptionType.OTHER,
          "Error reading " + path + " " + ioe.getMessage());
    }
  }

  @Override
  public void undo(long tid, Manager environment) throws Exception {
    MetadataTableUtil.deleteTable(tableInfo.tableId, false, environment.getContext(),
        environment.getManagerLock());
  }
}
