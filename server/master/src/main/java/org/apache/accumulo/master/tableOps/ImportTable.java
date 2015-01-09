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

import static com.google.common.base.Charsets.UTF_8;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.impl.Namespaces;
import org.apache.accumulo.core.client.impl.TableOperationsImpl;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.thrift.TableOperation;
import org.apache.accumulo.core.client.impl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.client.impl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.client.impl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.util.FastFormat;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.security.AuditedSecurityOperation;
import org.apache.accumulo.server.security.SecurityOperation;
import org.apache.accumulo.server.security.SystemCredentials;
import org.apache.accumulo.server.tables.TableManager;
import org.apache.accumulo.server.tablets.UniqueNameAllocator;
import org.apache.accumulo.server.util.MetadataTableUtil;
import org.apache.accumulo.server.util.TablePropUtil;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

/**
 *
 */
class ImportedTableInfo implements Serializable {

  private static final long serialVersionUID = 1L;

  public String exportDir;
  public String user;
  public String tableName;
  public String tableId;
  public String importDir;
  public String namespaceId;
}

class FinishImportTable extends MasterRepo {

  private static final long serialVersionUID = 1L;

  private ImportedTableInfo tableInfo;

  public FinishImportTable(ImportedTableInfo ti) {
    this.tableInfo = ti;
  }

  @Override
  public long isReady(long tid, Master environment) throws Exception {
    return 0;
  }

  @Override
  public Repo<Master> call(long tid, Master env) throws Exception {

    env.getFileSystem().deleteRecursively(new Path(tableInfo.importDir, "mappings.txt"));

    TableManager.getInstance().transitionTableState(tableInfo.tableId, TableState.ONLINE);

    Utils.unreserveNamespace(tableInfo.namespaceId, tid, false);
    Utils.unreserveTable(tableInfo.tableId, tid, true);

    Utils.unreserveHdfsDirectory(new Path(tableInfo.exportDir).toString(), tid);

    env.getEventCoordinator().event("Imported table %s ", tableInfo.tableName);

    Logger.getLogger(FinishImportTable.class).debug("Imported table " + tableInfo.tableId + " " + tableInfo.tableName);

    return null;
  }

  @Override
  public String getReturn() {
    return tableInfo.tableId;
  }

  @Override
  public void undo(long tid, Master env) throws Exception {}

}

class MoveExportedFiles extends MasterRepo {

  private static final long serialVersionUID = 1L;

  private ImportedTableInfo tableInfo;

  MoveExportedFiles(ImportedTableInfo ti) {
    this.tableInfo = ti;
  }

  @Override
  public Repo<Master> call(long tid, Master master) throws Exception {
    try {
      VolumeManager fs = master.getFileSystem();

      Map<String,String> fileNameMappings = PopulateMetadataTable.readMappingFile(fs, tableInfo);

      for (String oldFileName : fileNameMappings.keySet()) {
        if (!fs.exists(new Path(tableInfo.exportDir, oldFileName))) {
          throw new ThriftTableOperationException(tableInfo.tableId, tableInfo.tableName, TableOperation.IMPORT, TableOperationExceptionType.OTHER,
              "File referenced by exported table does not exists " + oldFileName);
        }
      }

      FileStatus[] files = fs.listStatus(new Path(tableInfo.exportDir));

      for (FileStatus fileStatus : files) {
        String newName = fileNameMappings.get(fileStatus.getPath().getName());

        if (newName != null)
          fs.rename(fileStatus.getPath(), new Path(tableInfo.importDir, newName));
      }

      return new FinishImportTable(tableInfo);
    } catch (IOException ioe) {
      log.warn(ioe.getMessage(), ioe);
      throw new ThriftTableOperationException(tableInfo.tableId, tableInfo.tableName, TableOperation.IMPORT, TableOperationExceptionType.OTHER,
          "Error renaming files " + ioe.getMessage());
    }
  }
}

class PopulateMetadataTable extends MasterRepo {

  private static final long serialVersionUID = 1L;

  private ImportedTableInfo tableInfo;

  PopulateMetadataTable(ImportedTableInfo ti) {
    this.tableInfo = ti;
  }

  static Map<String,String> readMappingFile(VolumeManager fs, ImportedTableInfo tableInfo) throws Exception {
    BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(new Path(tableInfo.importDir, "mappings.txt")), UTF_8));

    try {
      Map<String,String> map = new HashMap<String,String>();

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
            Text metadataRow = new KeyExtent(new Text(tableInfo.tableId), endRow, null).getMetadataEntry();

            Text cq;

            if (key.getColumnFamily().equals(DataFileColumnFamily.NAME)) {
              String oldName = new Path(key.getColumnQualifier().toString()).getName();
              String newName = fileNameMappings.get(oldName);

              if (newName == null) {
                throw new ThriftTableOperationException(tableInfo.tableId, tableInfo.tableName, TableOperation.IMPORT, TableOperationExceptionType.OTHER,
                    "File " + oldName + " does not exist in import dir");
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
      log.warn(ioe.getMessage(), ioe);
      throw new ThriftTableOperationException(tableInfo.tableId, tableInfo.tableName, TableOperation.IMPORT, TableOperationExceptionType.OTHER,
          "Error reading " + path + " " + ioe.getMessage());
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
    String tableDir = master.getFileSystem().choose(tableDirs);

    // Build up a full hdfs://localhost:8020/accumulo/tables/$id/c-XXXXXXX
    return tableDir + "/" + tableInfo.tableId + "/" + tabletDir;
  }

  @Override
  public void undo(long tid, Master environment) throws Exception {
    MetadataTableUtil.deleteTable(tableInfo.tableId, false, SystemCredentials.get(), environment.getMasterLock());
  }
}

class MapImportFileNames extends MasterRepo {

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
      log.warn(ioe.getMessage(), ioe);
      throw new ThriftTableOperationException(tableInfo.tableId, tableInfo.tableName, TableOperation.IMPORT, TableOperationExceptionType.OTHER,
          "Error writing mapping file " + path + " " + ioe.getMessage());
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

class CreateImportDir extends MasterRepo {
  private static final Logger log = Logger.getLogger(CreateImportDir.class);
  private static final long serialVersionUID = 1L;

  private ImportedTableInfo tableInfo;

  CreateImportDir(ImportedTableInfo ti) {
    this.tableInfo = ti;
  }

  @Override
  public Repo<Master> call(long tid, Master master) throws Exception {

    UniqueNameAllocator namer = UniqueNameAllocator.getInstance();

    Path exportDir = new Path(tableInfo.exportDir);
    String[] tableDirs = ServerConstants.getTablesDirs();

    log.info("Looking for matching filesystem for " + exportDir + " from options " + Arrays.toString(tableDirs));
    Path base = master.getFileSystem().matchingFileSystem(exportDir, tableDirs);
    log.info("Chose base table directory of " + base);
    Path directory = new Path(base, tableInfo.tableId);

    Path newBulkDir = new Path(directory, Constants.BULK_PREFIX + namer.getNextName());

    tableInfo.importDir = newBulkDir.toString();

    log.info("Using import dir: " + tableInfo.importDir);

    return new MapImportFileNames(tableInfo);
  }
}

class ImportPopulateZookeeper extends MasterRepo {

  private static final long serialVersionUID = 1L;

  private ImportedTableInfo tableInfo;

  ImportPopulateZookeeper(ImportedTableInfo ti) {
    this.tableInfo = ti;
  }

  @Override
  public long isReady(long tid, Master environment) throws Exception {
    return Utils.reserveTable(tableInfo.tableId, tid, true, false, TableOperation.IMPORT);
  }

  private Map<String,String> getExportedProps(VolumeManager fs) throws Exception {

    Path path = new Path(tableInfo.exportDir, Constants.EXPORT_FILE);

    try {
      FileSystem ns = fs.getVolumeByPath(path).getFileSystem();
      return TableOperationsImpl.getExportedProps(ns, path);
    } catch (IOException ioe) {
      throw new ThriftTableOperationException(tableInfo.tableId, tableInfo.tableName, TableOperation.IMPORT, TableOperationExceptionType.OTHER,
          "Error reading table props from " + path + " " + ioe.getMessage());
    }
  }

  @Override
  public Repo<Master> call(long tid, Master env) throws Exception {
    // reserve the table name in zookeeper or fail

    Utils.tableNameLock.lock();
    try {
      // write tableName & tableId to zookeeper
      Instance instance = HdfsZooInstance.getInstance();

      Utils.checkTableDoesNotExist(instance, tableInfo.tableName, tableInfo.tableId, TableOperation.CREATE);

      String namespace = Tables.qualify(tableInfo.tableName).getFirst();
      String namespaceId = Namespaces.getNamespaceId(instance, namespace);
      TableManager.getInstance().addTable(tableInfo.tableId, namespaceId, tableInfo.tableName, NodeExistsPolicy.OVERWRITE);

      Tables.clearCache(instance);
    } finally {
      Utils.tableNameLock.unlock();
    }

    for (Entry<String,String> entry : getExportedProps(env.getFileSystem()).entrySet())
      if (!TablePropUtil.setTableProperty(tableInfo.tableId, entry.getKey(), entry.getValue())) {
        throw new ThriftTableOperationException(tableInfo.tableId, tableInfo.tableName, TableOperation.IMPORT, TableOperationExceptionType.OTHER,
            "Invalid table property " + entry.getKey());
      }

    return new CreateImportDir(tableInfo);
  }

  @Override
  public void undo(long tid, Master env) throws Exception {
    Instance instance = HdfsZooInstance.getInstance();
    TableManager.getInstance().removeTable(tableInfo.tableId);
    Utils.unreserveTable(tableInfo.tableId, tid, true);
    Tables.clearCache(instance);
  }
}

class ImportSetupPermissions extends MasterRepo {

  private static final long serialVersionUID = 1L;

  private ImportedTableInfo tableInfo;

  public ImportSetupPermissions(ImportedTableInfo ti) {
    this.tableInfo = ti;
  }

  @Override
  public long isReady(long tid, Master environment) throws Exception {
    return 0;
  }

  @Override
  public Repo<Master> call(long tid, Master env) throws Exception {
    // give all table permissions to the creator
    SecurityOperation security = AuditedSecurityOperation.getInstance();
    for (TablePermission permission : TablePermission.values()) {
      try {
        security
            .grantTablePermission(SystemCredentials.get().toThrift(env.getInstance()), tableInfo.user, tableInfo.tableId, permission, tableInfo.namespaceId);
      } catch (ThriftSecurityException e) {
        Logger.getLogger(ImportSetupPermissions.class).error(e.getMessage(), e);
        throw e;
      }
    }

    // setup permissions in zookeeper before table info in zookeeper
    // this way concurrent users will not get a spurious permission denied
    // error
    return new ImportPopulateZookeeper(tableInfo);
  }

  @Override
  public void undo(long tid, Master env) throws Exception {
    AuditedSecurityOperation.getInstance().deleteTable(SystemCredentials.get().toThrift(env.getInstance()), tableInfo.tableId, tableInfo.namespaceId);
  }
}

public class ImportTable extends MasterRepo {
  private static final long serialVersionUID = 1L;

  private ImportedTableInfo tableInfo;

  public ImportTable(String user, String tableName, String exportDir, String namespaceId) {
    tableInfo = new ImportedTableInfo();
    tableInfo.tableName = tableName;
    tableInfo.user = user;
    tableInfo.exportDir = exportDir;
    tableInfo.namespaceId = namespaceId;
  }

  @Override
  public long isReady(long tid, Master environment) throws Exception {
    return Utils.reserveHdfsDirectory(new Path(tableInfo.exportDir).toString(), tid)
        + Utils.reserveNamespace(tableInfo.namespaceId, tid, false, true, TableOperation.IMPORT);
  }

  @Override
  public Repo<Master> call(long tid, Master env) throws Exception {
    checkVersions(env);

    // first step is to reserve a table id.. if the machine fails during this step
    // it is ok to retry... the only side effect is that a table id may not be used
    // or skipped

    // assuming only the master process is creating tables

    Utils.idLock.lock();
    try {
      Instance instance = HdfsZooInstance.getInstance();
      tableInfo.tableId = Utils.getNextTableId(tableInfo.tableName, instance);
      return new ImportSetupPermissions(tableInfo);
    } finally {
      Utils.idLock.unlock();
    }
  }

  public void checkVersions(Master env) throws ThriftTableOperationException {
    Path path = new Path(tableInfo.exportDir, Constants.EXPORT_FILE);

    ZipInputStream zis = null;

    try {
      zis = new ZipInputStream(env.getFileSystem().open(path));

      Integer exportVersion = null;
      Integer dataVersion = null;

      ZipEntry zipEntry;
      while ((zipEntry = zis.getNextEntry()) != null) {
        if (zipEntry.getName().equals(Constants.EXPORT_INFO_FILE)) {
          BufferedReader in = new BufferedReader(new InputStreamReader(zis, UTF_8));
          String line = null;
          while ((line = in.readLine()) != null) {
            String sa[] = line.split(":", 2);
            if (sa[0].equals(ExportTable.EXPORT_VERSION_PROP)) {
              exportVersion = Integer.parseInt(sa[1]);
            } else if (sa[0].equals(ExportTable.DATA_VERSION_PROP)) {
              dataVersion = Integer.parseInt(sa[1]);
            }
          }

          break;
        }
      }

      zis.close();
      zis = null;

      if (exportVersion == null || exportVersion > ExportTable.VERSION)
        throw new ThriftTableOperationException(null, tableInfo.tableName, TableOperation.IMPORT, TableOperationExceptionType.OTHER,
            "Incompatible export version " + exportVersion);

      if (dataVersion == null || dataVersion > ServerConstants.DATA_VERSION)
        throw new ThriftTableOperationException(null, tableInfo.tableName, TableOperation.IMPORT, TableOperationExceptionType.OTHER,
            "Incompatible data version " + exportVersion);

    } catch (IOException ioe) {
      log.warn(ioe.getMessage(), ioe);
      throw new ThriftTableOperationException(null, tableInfo.tableName, TableOperation.IMPORT, TableOperationExceptionType.OTHER,
          "Failed to read export metadata " + ioe.getMessage());
    } finally {
      if (zis != null)
        try {
          zis.close();
        } catch (IOException ioe) {
          log.warn(ioe.getMessage(), ioe);
        }
    }
  }

  @Override
  public void undo(long tid, Master env) throws Exception {
    Utils.unreserveHdfsDirectory(new Path(tableInfo.exportDir).toString(), tid);
    Utils.unreserveNamespace(tableInfo.namespaceId, tid, false);
  }
}
