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
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.impl.AcceptableThriftTableOperationException;
import org.apache.accumulo.core.client.impl.thrift.TableOperation;
import org.apache.accumulo.core.client.impl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.util.FastFormat;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.fs.VolumeChooserEnvironment;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.util.MetadataTableUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PopulateMetadataTable extends MasterRepo {
  private static final Logger log = LoggerFactory.getLogger(PopulateMetadataTable.class);

  private static final long serialVersionUID = 1L;

  private ImportedTableInfo tableInfo;

  PopulateMetadataTable(ImportedTableInfo ti) {
    this.tableInfo = ti;
  }

  static Map<String,String> readMappingFile(VolumeManager fs, ImportedTableInfo tableInfo) throws Exception {
    BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(new Path(tableInfo.importDir, "mappings.txt")), UTF_8));

    try {
      Map<String,String> map = new HashMap<>();

      String line = null;
      while ((line = in.readLine()) != null) {
        String sa[] = line.split(":", 2);
        map.put(sa[0], sa[1]);
      }

      return map;
    } finally {
      in.close();
    }

  }

  @Override
  public Repo<Master> call(long tid, Master master) throws Exception {

    Path path = new Path(tableInfo.exportDir, Constants.EXPORT_FILE);

    BatchWriter mbw = null;
    ZipInputStream zis = null;

    try {
      VolumeManager fs = master.getFileSystem();

      mbw = master.getConnector().createBatchWriter(MetadataTable.NAME, new BatchWriterConfig());

      zis = new ZipInputStream(fs.open(path));

      Map<String,String> fileNameMappings = readMappingFile(fs, tableInfo);

      log.info("importDir is " + tableInfo.importDir);

      // This is a directory already prefixed with proper volume information e.g. hdfs://localhost:8020/path/to/accumulo/tables/...
      final String bulkDir = tableInfo.importDir;

      final String[] tableDirs = ServerConstants.getTablesDirs();

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

            Text endRow = new KeyExtent(key.getRow(), (Text) null).getEndRow();
            Text metadataRow = new KeyExtent(tableInfo.tableId, endRow, null).getMetadataEntry();

            Text cq;

            if (key.getColumnFamily().equals(DataFileColumnFamily.NAME)) {
              String oldName = new Path(key.getColumnQualifier().toString()).getName();
              String newName = fileNameMappings.get(oldName);

              if (newName == null) {
                throw new AcceptableThriftTableOperationException(tableInfo.tableId.canonicalID(), tableInfo.tableName, TableOperation.IMPORT,
                    TableOperationExceptionType.OTHER, "File " + oldName + " does not exist in import dir");
              }

              cq = new Text(bulkDir + "/" + newName);
            } else {
              cq = key.getColumnQualifier();
            }

            if (m == null) {
              // Make a unique directory inside the table's dir. Cannot import multiple tables into one table, so don't need to use unique allocator
              String tabletDir = new String(FastFormat.toZeroPaddedString(dirCount++, 8, 16, Constants.CLONE_PREFIX_BYTES), UTF_8);

              // Build up a full hdfs://localhost:8020/accumulo/tables/$id/c-XXXXXXX
              String absolutePath = getClonedTabletDir(master, tableDirs, tabletDir);

              m = new Mutation(metadataRow);
              TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN.put(m, new Value(absolutePath.getBytes(UTF_8)));
              currentRow = metadataRow;
            }

            if (!currentRow.equals(metadataRow)) {
              mbw.addMutation(m);

              // Make a unique directory inside the table's dir. Cannot import multiple tables into one table, so don't need to use unique allocator
              String tabletDir = new String(FastFormat.toZeroPaddedString(dirCount++, 8, 16, Constants.CLONE_PREFIX_BYTES), UTF_8);

              // Build up a full hdfs://localhost:8020/accumulo/tables/$id/c-XXXXXXX
              String absolutePath = getClonedTabletDir(master, tableDirs, tabletDir);

              m = new Mutation(metadataRow);
              TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN.put(m, new Value(absolutePath.getBytes(UTF_8)));
            }

            m.put(key.getColumnFamily(), cq, val);

            if (endRow == null && TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.hasColumns(key)) {
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
      throw new AcceptableThriftTableOperationException(tableInfo.tableId.canonicalID(), tableInfo.tableName, TableOperation.IMPORT,
          TableOperationExceptionType.OTHER, "Error reading " + path + " " + ioe.getMessage());
    } finally {
      if (zis != null) {
        try {
          zis.close();
        } catch (IOException ioe) {
          log.warn("Failed to close zip file ", ioe);
        }
      }

      if (mbw != null) {
        mbw.close();
      }
    }
  }

  /**
   * Given options for tables (across multiple volumes), construct an absolute path using the unique name within the chosen volume
   *
   * @return An absolute, unique path for the imported table
   */
  protected String getClonedTabletDir(Master master, String[] tableDirs, String tabletDir) {
    // We can try to spread out the tablet dirs across all volumes
    VolumeChooserEnvironment chooserEnv = new VolumeChooserEnvironment(tableInfo.tableId);
    String tableDir = master.getFileSystem().choose(chooserEnv, tableDirs);

    // Build up a full hdfs://localhost:8020/accumulo/tables/$id/c-XXXXXXX
    return tableDir + "/" + tableInfo.tableId + "/" + tabletDir;
  }

  @Override
  public void undo(long tid, Master environment) throws Exception {
    MetadataTableUtil.deleteTable(tableInfo.tableId, false, environment, environment.getMasterLock());
  }
}
