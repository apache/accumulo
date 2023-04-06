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
package org.apache.accumulo.manager.tableOps.tableExport;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.clientImpl.AcceptableThriftTableOperationException;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.clientImpl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.ValidationUtil;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.CurrentLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.FutureLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.LogColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.manager.tableOps.Utils;
import org.apache.accumulo.server.AccumuloDataVersion;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

class WriteExportFiles extends ManagerRepo {

  private static final long serialVersionUID = 1L;
  private final ExportInfo tableInfo;

  WriteExportFiles(ExportInfo tableInfo) {
    this.tableInfo = tableInfo;
  }

  private void checkOffline(ClientContext context) throws Exception {
    if (context.getTableState(tableInfo.tableID) != TableState.OFFLINE) {
      context.clearTableListCache();
      if (context.getTableState(tableInfo.tableID) != TableState.OFFLINE) {
        throw new AcceptableThriftTableOperationException(tableInfo.tableID.canonical(),
            tableInfo.tableName, TableOperation.EXPORT, TableOperationExceptionType.OTHER,
            "Table is not offline");
      }
    }
  }

  @Override
  public long isReady(long tid, Manager manager) throws Exception {

    long reserved = Utils.reserveNamespace(manager, tableInfo.namespaceID, tid, false, true,
        TableOperation.EXPORT)
        + Utils.reserveTable(manager, tableInfo.tableID, tid, false, true, TableOperation.EXPORT);
    if (reserved > 0) {
      return reserved;
    }

    AccumuloClient client = manager.getContext();

    checkOffline(manager.getContext());

    Scanner metaScanner = client.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    metaScanner.setRange(new KeyExtent(tableInfo.tableID, null, null).toMetaRange());

    // scan for locations
    metaScanner.fetchColumnFamily(CurrentLocationColumnFamily.NAME);
    metaScanner.fetchColumnFamily(FutureLocationColumnFamily.NAME);

    if (metaScanner.iterator().hasNext()) {
      return 500;
    }

    // use the same range to check for walogs that we used to check for hosted (or future hosted)
    // tablets
    // this is done as a separate scan after we check for locations, because walogs are okay only if
    // there is no location
    metaScanner.clearColumns();
    metaScanner.fetchColumnFamily(LogColumnFamily.NAME);

    if (metaScanner.iterator().hasNext()) {
      throw new AcceptableThriftTableOperationException(tableInfo.tableID.canonical(),
          tableInfo.tableName, TableOperation.EXPORT, TableOperationExceptionType.OTHER,
          "Write ahead logs found for table");
    }

    return 0;
  }

  @Override
  public Repo<Manager> call(long tid, Manager manager) throws Exception {
    try {
      exportTable(manager.getVolumeManager(), manager.getContext(), tableInfo.tableName,
          tableInfo.tableID, tableInfo.exportDir);
    } catch (IOException ioe) {
      throw new AcceptableThriftTableOperationException(tableInfo.tableID.canonical(),
          tableInfo.tableName, TableOperation.EXPORT, TableOperationExceptionType.OTHER,
          "Failed to create export files " + ioe.getMessage());
    }
    Utils.unreserveNamespace(manager, tableInfo.namespaceID, tid, false);
    Utils.unreserveTable(manager, tableInfo.tableID, tid, false);
    Utils.unreserveHdfsDirectory(manager, new Path(tableInfo.exportDir).toString(), tid);
    return null;
  }

  @Override
  public void undo(long tid, Manager env) {
    Utils.unreserveNamespace(env, tableInfo.namespaceID, tid, false);
    Utils.unreserveTable(env, tableInfo.tableID, tid, false);
  }

  public static void exportTable(VolumeManager fs, ServerContext context, String tableName,
      TableId tableID, String exportDir) throws Exception {

    fs.mkdirs(new Path(exportDir));
    Path exportMetaFilePath = fs.getFileSystemByPath(new Path(exportDir))
        .makeQualified(new Path(exportDir, Constants.EXPORT_FILE));

    FSDataOutputStream fileOut = fs.create(exportMetaFilePath);
    ZipOutputStream zipOut = new ZipOutputStream(fileOut);
    BufferedOutputStream bufOut = new BufferedOutputStream(zipOut);
    DataOutputStream dataOut = new DataOutputStream(bufOut);

    try (OutputStreamWriter osw = new OutputStreamWriter(dataOut, UTF_8)) {

      zipOut.putNextEntry(new ZipEntry(Constants.EXPORT_INFO_FILE));
      osw.append(ExportTable.EXPORT_VERSION_PROP + ":" + ExportTable.VERSION + "\n");
      osw.append("srcInstanceName:" + context.getInstanceName() + "\n");
      osw.append("srcInstanceID:" + context.getInstanceID() + "\n");
      osw.append("srcZookeepers:" + context.getZooKeepers() + "\n");
      osw.append("srcTableName:" + tableName + "\n");
      osw.append("srcTableID:" + tableID.canonical() + "\n");
      osw.append(ExportTable.DATA_VERSION_PROP + ":" + AccumuloDataVersion.get() + "\n");
      osw.append("srcCodeVersion:" + Constants.VERSION + "\n");

      osw.flush();
      dataOut.flush();

      exportConfig(context, tableID, zipOut, dataOut);
      dataOut.flush();

      Map<String,String> uniqueFiles = exportMetadata(fs, context, tableID, zipOut, dataOut);

      dataOut.close();
      dataOut = null;

      createDistcpFile(fs, exportDir, exportMetaFilePath, uniqueFiles);

    } finally {
      if (dataOut != null) {
        dataOut.close();
      }
    }
  }

  private static void createDistcpFile(VolumeManager fs, String exportDir, Path exportMetaFilePath,
      Map<String,String> uniqueFiles) throws IOException {
    BufferedWriter distcpOut = new BufferedWriter(
        new OutputStreamWriter(fs.create(new Path(exportDir, "distcp.txt")), UTF_8));

    try {
      for (String file : uniqueFiles.values()) {
        distcpOut.append(file);
        distcpOut.newLine();
      }

      distcpOut.append(exportMetaFilePath.toString());
      distcpOut.newLine();

      distcpOut.close();
      distcpOut = null;

    } finally {
      if (distcpOut != null) {
        distcpOut.close();
      }
    }
  }

  private static Map<String,String> exportMetadata(VolumeManager fs, ServerContext context,
      TableId tableID, ZipOutputStream zipOut, DataOutputStream dataOut)
      throws IOException, TableNotFoundException {
    zipOut.putNextEntry(new ZipEntry(Constants.EXPORT_METADATA_FILE));

    Map<String,String> uniqueFiles = new HashMap<>();

    Scanner metaScanner = context.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    metaScanner.fetchColumnFamily(DataFileColumnFamily.NAME);
    TabletColumnFamily.PREV_ROW_COLUMN.fetch(metaScanner);
    ServerColumnFamily.TIME_COLUMN.fetch(metaScanner);
    metaScanner.setRange(new KeyExtent(tableID, null, null).toMetaRange());

    for (Entry<Key,Value> entry : metaScanner) {
      entry.getKey().write(dataOut);
      entry.getValue().write(dataOut);

      if (entry.getKey().getColumnFamily().equals(DataFileColumnFamily.NAME)) {
        String path = ValidationUtil.validate(entry.getKey().getColumnQualifierData().toString());
        String[] tokens = path.split("/");
        if (tokens.length < 1) {
          throw new RuntimeException("Illegal path " + path);
        }

        String filename = tokens[tokens.length - 1];

        String existingPath = uniqueFiles.get(filename);
        if (existingPath == null) {
          uniqueFiles.put(filename, path);
        } else if (!existingPath.equals(path)) {
          // make sure file names are unique, should only apply for tables with file names generated
          // by Accumulo 1.3 and earlier
          throw new IOException("Cannot export table with nonunique file names " + filename
              + ". Major compact table.");
        }

      }
    }
    return uniqueFiles;
  }

  private static void exportConfig(ServerContext context, TableId tableID, ZipOutputStream zipOut,
      DataOutputStream dataOut) throws AccumuloException, AccumuloSecurityException, IOException {

    DefaultConfiguration defaultConfig = DefaultConfiguration.getInstance();
    Map<String,String> siteConfig = context.instanceOperations().getSiteConfiguration();
    Map<String,String> systemConfig = context.instanceOperations().getSystemConfiguration();

    TableConfiguration tableConfig = context.getTableConfiguration(tableID);

    OutputStreamWriter osw = new OutputStreamWriter(dataOut, UTF_8);

    // only put props that are different than defaults and higher level configurations
    zipOut.putNextEntry(new ZipEntry(Constants.EXPORT_TABLE_CONFIG_FILE));
    for (Entry<String,String> prop : tableConfig) {
      if (prop.getKey().startsWith(Property.TABLE_PREFIX.getKey())) {
        Property key = Property.getPropertyByKey(prop.getKey());

        if (key == null || !defaultConfig.get(key).equals(prop.getValue())) {
          if (!prop.getValue().equals(siteConfig.get(prop.getKey()))
              && !prop.getValue().equals(systemConfig.get(prop.getKey()))) {
            osw.append(prop.getKey() + "=" + prop.getValue() + "\n");
          }
        }
      }
    }

    osw.flush();
  }
}
